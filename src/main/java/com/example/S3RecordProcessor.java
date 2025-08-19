package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.keyspaces.streamsadapter.model.KeyspacesStreamsProcessRecordsInput;
import software.amazon.keyspaces.streamsadapter.processor.KeyspacesStreamsShardRecordProcessor;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3RecordProcessor implements KeyspacesStreamsShardRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(S3RecordProcessor.class);
    private static final Pattern KEYSPACES_STREAM_ARN_PATTERN = Pattern.compile(
            "arn:aws(?:-cn)?:cassandra:(?<region>[-a-z0-9]+):(?<accountId>[0-9]{12}):/keyspace/(?<keyspaceName>[^/]+)/table/(?<tableName>[^/]+)/stream/(?<streamLabel>.+)");
    
    private final S3Client s3Client;
    private final String bucketName;
    private final String keyspaceName;
    private final String tableName;
    private final boolean base64Encode;
    private final ObjectMapper objectMapper;

    public S3RecordProcessor(S3Client s3Client, String bucketName, String streamArn, boolean base64Encode) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.base64Encode = base64Encode;
        this.objectMapper = new ObjectMapper();
        
        Matcher matcher = KEYSPACES_STREAM_ARN_PATTERN.matcher(streamArn);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid Keyspaces stream ARN format: " + streamArn);
        }
        this.keyspaceName = matcher.group("keyspaceName");
        this.tableName = matcher.group("tableName");
    }

    public S3RecordProcessor(S3Client s3Client, String bucketName, String streamArn) {
        this(s3Client, bucketName, streamArn, false);
    }

    @Override
    public void processRecords(KeyspacesStreamsProcessRecordsInput processRecordsInput) {
        List<KeyspacesStreamsClientRecord> records = processRecordsInput.records();
        logger.info("Processing {} records", records.size());
        
        for (KeyspacesStreamsClientRecord record : records) {
            try {
                String json = StandardCharsets.UTF_8.decode(record.data()).toString();
                String content = base64Encode ? Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)) : json;
                
                String key = String.format("%s/%s/%s/%s.json", 
                    keyspaceName, tableName,
                    Instant.now().toString().substring(0, 10), 
                    record.sequenceNumber());
                
                // Retry S3 write up to 3 times
                boolean success = false;
                for (int attempt = 1; attempt <= 3 && !success; attempt++) {
                    try {
                        s3Client.putObject(
                            PutObjectRequest.builder()
                                .bucket(bucketName)
                                .key(key)
                                .build(),
                            RequestBody.fromString(content)
                        );
                        success = true;
                        logger.debug("Written record to S3: {}", key);
                    } catch (Exception s3Error) {
                        logger.warn("S3 write attempt {} failed: {}", attempt, s3Error.getMessage());
                        if (attempt < 3) {
                            Thread.sleep(1000 * attempt); // Exponential backoff
                        } else {
                            throw s3Error;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to write record {} to S3: {}", record.sequenceNumber(), e.getMessage(), e);
                // Continue processing other records
            }
        }
        
        try {
            processRecordsInput.checkpointer().checkpoint();
            logger.debug("Checkpoint successful for {} records", records.size());
        } catch (Exception e) {
            logger.error("CRITICAL: Failed to checkpoint: {}", e.getMessage(), e);
            // Don't throw - this would kill the processor
        }
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        logger.info("Initializing processor for shard: {}", initializationInput.shardId());
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {}

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            logger.error("Failed to checkpoint at shard end: {}", e.getMessage(), e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            logger.error("Failed to checkpoint at shutdown: {}", e.getMessage(), e);
        }
    }
}