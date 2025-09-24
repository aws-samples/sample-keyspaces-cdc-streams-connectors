package software.amazon.ssa.streams.processor;

import software.amazon.keyspaces.streamsadapter.model.KeyspacesStreamsProcessRecordsInput;
import software.amazon.keyspaces.streamsadapter.processor.KeyspacesStreamsShardRecordProcessor;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.ITargetMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyspacesRecordProcessor implements KeyspacesStreamsShardRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(KeyspacesRecordProcessor.class);
    
    private String shardId;
    
    ITargetMapper targetMapper;
    KeyspacesConfig keyspacesConfig;

    public KeyspacesRecordProcessor(ITargetMapper targetMapper, KeyspacesConfig keyspacesConfig) {
        this.keyspacesConfig = keyspacesConfig;
        this.targetMapper = targetMapper;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        this.shardId = initializationInput.shardId();

        targetMapper.initialize(keyspacesConfig);
        logger.info("Initializing record processor for shard: {}", shardId);
    }

    @Override
    public void processRecords(KeyspacesStreamsProcessRecordsInput processRecordsInput) {
        try {
            
            targetMapper.handleRecords(processRecordsInput.records());
            
            if (!processRecordsInput.records().isEmpty()) {
                RecordProcessorCheckpointer checkpointer = processRecordsInput.checkpointer();
                try {
                    checkpointer.checkpoint();
                    logger.debug("Checkpoint successful for shard: {}", shardId);
                } catch (Exception e) {
                    logger.error("Error while checkpointing for shard: {} - {}", shardId, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing records for shard: {} - {}", shardId, e.getMessage(), e);
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        logger.warn("Lease lost for shard: {}", shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        logger.info("Shard ended: {}", shardId);
        try {
            // This is required. Checkpoint at the end of the shard
            shardEndedInput.checkpointer().checkpoint();
            logger.debug("Final checkpoint successful for shard: {}", shardId);
        } catch (Exception e) {
            logger.error("Error while final checkpointing for shard: {} - {}", shardId, e.getMessage(), e);
            throw new RuntimeException("Error while final checkpointing", e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        logger.info("Shutdown requested for shard {}", shardId);
        try {
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            logger.error("Error while checkpointing on shutdown for shard: {} - {}", shardId, e.getMessage(), e);
        }
    }
} 