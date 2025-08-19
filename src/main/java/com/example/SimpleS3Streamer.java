package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.keyspaces.streamsadapter.AmazonKeyspacesStreamsAdapterClient;
import software.amazon.keyspaces.streamsadapter.StreamsSchedulerFactory;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

public class SimpleS3Streamer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleS3Streamer.class);
    
    public static void main(String[] args) {
        if (args.length < 3) {
            logger.error("Usage: java SimpleS3Streamer <keyspaces-stream-arn> <s3-bucket-name> <region> [base64-encode:true|false]");
            System.exit(1);
        }

        String streamArn = args[0];
        String bucketName = args[1];
        Region region = Region.of(args[2]);
        boolean base64Encode = args.length > 3 ? Boolean.parseBoolean(args[3]) : false;
        
        // Extract keyspace and table name from ARN
        // ARN format: arn:aws:cassandra:region:account:/keyspace/KEYSPACE_NAME/table/TABLE_NAME/stream/STREAM_LABEL
        String[] arnParts = streamArn.split("/");
        String keyspaceName = arnParts.length > 2 ? arnParts[2] : "unknown";
        String tableName = arnParts.length > 4 ? arnParts[4] : "unknown";
        
        // Create consistent DynamoDB table name (no random salt to preserve checkpoints)
        String dynamoTableName = "keyspaces-streamer-" + keyspaceName + "-" + tableName;
        String workerId = "worker-" + System.getenv().getOrDefault("HOSTNAME", "default");

        S3Client s3Client = S3Client.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

        AmazonKeyspacesStreamsAdapterClient adapterClient = 
            new AmazonKeyspacesStreamsAdapterClient(
                DefaultCredentialsProvider.create(),
                region
            );

        StreamTracker streamTracker = StreamsSchedulerFactory.createSingleStreamTracker(
            streamArn, 
            InitialPositionInStreamExtended.newInitialPosition(
                software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON)
        );

        RetrievalConfig retrievalConfig = new RetrievalConfig(
            adapterClient,
            streamTracker,
            dynamoTableName
        );
        retrievalConfig.retrievalSpecificConfig(new PollingConfig(streamArn, adapterClient));

        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
            new CheckpointConfig(),
            new CoordinatorConfig(dynamoTableName),
            new LeaseManagementConfig(
                dynamoTableName,
                dynamoTableName,
                software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient.builder()
                    .region(region)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build(),
                adapterClient,
                workerId
            ),
            new LifecycleConfig(),
            new MetricsConfig(
                software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient.builder()
                    .region(region)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build(),
                dynamoTableName
            ),
            new ProcessorConfig(
                new ShardRecordProcessorFactory() {
                    @Override
                    public software.amazon.kinesis.processor.ShardRecordProcessor shardRecordProcessor() {
                        return new S3RecordProcessor(s3Client, bucketName, streamArn, base64Encode);
                    }
                }
            ),
            retrievalConfig,
            DefaultCredentialsProvider.create(),
            region
        );

        logger.info("Starting Keyspaces to S3 streamer...");
        logger.info("Keyspace: {}, Table: {}", keyspaceName, tableName);
        logger.info("DynamoDB table '{}' will be created in region: {}", dynamoTableName, region);
        logger.info("Worker ID: {}", workerId);
        logger.info("S3 bucket: {}", bucketName);
        scheduler.run();
    }
}