package software.amazon.ssa.streams.connector.sqs;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;

// removed unused imports
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.converters.IStreamRecordConverter;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.AbstractTargetMapper;
import software.amazon.ssa.streams.exception.PartialFailureException;
import software.amazon.ssa.streams.exception.AllItemsFailureException;

/**
 * SQS Target Mapper for Amazon Keyspaces CDC Streams
 * 
 * This connector writes Keyspaces CDC records to Amazon SQS queues in JSON format.
 * It supports configurable message formatting, retry logic, and batch processing for reliable message delivery.
 * 
 * Configuration:
 * - queue-url: SQS queue URL (required)
 * - region: AWS region (default: us-east-1)
 * - message-format: Message format - "full", "new-image", "old-image", or "fields-only" (default: full)
 * - fields-to-include: List of fields to include in messages (optional, used with fields-only format)
 * - include-metadata: Include CDC metadata in messages (default: true)
 * - max-retries: Maximum retry attempts for SQS operations (default: 3)
 * - delay-seconds: Message delay in seconds (default: 0)
 */
@SuppressWarnings("UUF_UNUSED_FIELD")
public class SQSTargetMapper extends  AbstractTargetMapper {

    private static final Logger logger = LoggerFactory.getLogger(SQSTargetMapper.class);
    
    private String queueUrl;
    private String region;
    private int delaySeconds;
    private String keyspaceName;
    private String tableName;

    IStreamRecordConverter<SendMessageBatchRequestEntry> jsonConverter;
    
    @SuppressWarnings("CT_CONSTRUCTOR_THROW")
    public SQSTargetMapper(Config config) {
        super(config);
        
        try {
            this.keyspaceName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.keyspace-name", "", true);
            this.tableName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.table-name", "", true);
            this.queueUrl = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.connector.queue-url", "", true);
            this.region = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.connector.region", "us-east-1", false);
            this.delaySeconds = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.delay-seconds", 0, false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SQS Target Mapper", e);
        }
    }

    
    @Override
    public void initialize() {
        super.initialize();
        jsonConverter = new SQSJsonConverter(config, delaySeconds);
        
    }

    
    @Override
    public void handleRecords(List<KeyspacesStreamsClientRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            logger.debug("No records to process");
            return;
        }

        // Use JSONHelper to create messages (records per message) and batches (messages per batch)
        // SQS limits: 256KB per message, 10 messages per batch. We'll leave batch size unlimited (-1).
        List<SendMessageBatchRequestEntry> requestEntries = jsonConverter.convertRecordsToMessages(
            records,
            keyspaceName,
            tableName         // maxRecordsPerMessage: unlimited      // maxMessagesPerBatch: 10
        );

        int batchSize = 10;

        List<SendMessageBatchRequest> batches = IntStream.range(0, (requestEntries.size() + batchSize - 1) / batchSize)
        .mapToObj(i -> requestEntries.subList(i * batchSize, Math.min(requestEntries.size(), (i + 1) * batchSize)))
        .map(batch -> {
            SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(batch)
                .build();
            return batchRequest;
        })
        .collect(Collectors.toList());

        for(SendMessageBatchRequest batchRequest : batches){
            sendBatchMessage(batchRequest);
        }

        logger.info("Successfully processed {} records to SQS queue: {})",
            records.size(), queueUrl);
    }

    protected void sendBatchMessage(SendMessageBatchRequest batchRequest) throws Exception {             
               
        SendMessageBatchResponse response = SQSService.getInstance(region).sendBatchRequest(batchRequest);
        
        int totalItems = batchRequest.entries().size();
        
        int failedItems = response.failed().size();
        
        if(totalItems > 0 && totalItems == failedItems ){
            // All items failed
            List<String> errorMessages = response.failed().stream()
                .map(failed -> String.format("Message ID %s: %s", failed.id(), failed.message()))
                .collect(Collectors.toList());

            logger.error("All messages failed to send to SQS. total messages: {}", totalItems);
            
            // Log individual failures
            for (var failed : response.failed()) {
                logger.error("Failed to send message ID {} to SQS: {}", failed.id(), failed.message());
            }
            
            // Throw AllItemsFailureException
            throw new AllItemsFailureException(SQSTargetMapper.class.getName(), errorMessages, totalItems);
            
        }else if(failedItems > 0){
            // Partial failure
            List<String> errorMessages = response.failed().stream()
                .map(failed -> String.format("Message ID %s: %s", failed.id(), failed.message()))
                .collect(Collectors.toList());
            
            logger.error("Partial failure in SQS batch: {} of {} messages failed", failedItems, totalItems);
            
            // Log individual failures
            for (var failed : response.failed()) {
                logger.error("Failed to send message ID {} to SQS: {}", failed.id(), failed.message());
            }
            
            // Throw PartialFailureException with detailed error information
            throw new PartialFailureException(SQSTargetMapper.class.getName(), errorMessages, totalItems, failedItems);
            
        }else{
            // All items succeeded
            logger.info("Successfully sent all messages to SQS. total messages: {}", totalItems);
        }
    }
}
