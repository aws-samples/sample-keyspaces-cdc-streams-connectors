package software.amazon.ssa.streams.processor;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.ITargetMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


public class KeyspacesRecordProcessorFactory implements ShardRecordProcessorFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(KeyspacesRecordProcessorFactory.class);
    
    private final Queue<KeyspacesRecordProcessor> processors = new ConcurrentLinkedQueue<>();
    private final KeyspacesConfig keyspacesConfig;
    
    private final ITargetMapper targetMapper;

    public KeyspacesRecordProcessorFactory(ITargetMapper targetMapper, KeyspacesConfig keyspacesConfig) {
        this.targetMapper = targetMapper;
        this.keyspacesConfig = keyspacesConfig;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        logger.debug("Creating new RecordProcessor");
        KeyspacesRecordProcessor processor = new KeyspacesRecordProcessor(targetMapper, keyspacesConfig);
        processors.add(processor);
        return processor;
    }
} 