package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.keyspaces.streamsadapter.StreamsSchedulerFactory;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.StreamTracker;

public class KeyspacesToS3Streamer {
    private static final Logger logger = LoggerFactory.getLogger(KeyspacesToS3Streamer.class);
    
    public static void main(String[] args) {
        if (args.length < 3) {
            logger.error("Usage: java KeyspacesToS3Streamer <keyspaces-stream-arn> <s3-bucket-name> <region>");
            System.exit(1);
        }

        String streamArn = args[0];
        String bucketName = args[1];
        Region region = Region.of(args[2]);

        S3Client s3Client = S3Client.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

        StreamTracker streamTracker = StreamsSchedulerFactory.createSingleStreamTracker(
            streamArn, 
            InitialPositionInStreamExtended.newInitialPosition(
                software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON)
        );

        software.amazon.kinesis.common.ConfigsBuilder configsBuilder = 
            new software.amazon.kinesis.common.ConfigsBuilder(
                streamTracker,
                "keyspaces-s3-streamer",
                software.amazon.awssdk.services.kinesis.KinesisAsyncClient.builder()
                    .region(region)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build(),
                software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient.builder()
                    .region(region)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build(),
                software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient.builder()
                    .region(region)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build(),
                "worker-id",
                new ShardRecordProcessorFactory() {
                    @Override
                    public software.amazon.kinesis.processor.ShardRecordProcessor shardRecordProcessor() {
                        return new S3RecordProcessor(s3Client, bucketName, streamArn, false);
                    }
                }
            );

        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            configsBuilder.leaseManagementConfig(),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig(),
            configsBuilder.retrievalConfig(),
            DefaultCredentialsProvider.create(),
            region
        );

        logger.info("Starting Keyspaces to S3 streamer for stream: {}", streamArn);
        scheduler.run();
    }
}