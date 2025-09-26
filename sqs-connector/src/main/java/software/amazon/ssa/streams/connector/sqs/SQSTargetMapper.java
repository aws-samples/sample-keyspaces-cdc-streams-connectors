package software.amazon.ssa.streams.connector.sqs;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.typesafe.config.Config;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.AbstractTargetMapper;
import software.amazon.ssa.streams.helpers.StreamHelpers;
import software.amazon.ssa.streams.helpers.StreamHelpers.StreamProcessorOperationType;
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
public class SQSTargetMapper extends  AbstractTargetMapper {

    private static final Logger logger = LoggerFactory.getLogger(SQSTargetMapper.class);
    
    private ObjectMapper JSON_MAPPER = new ObjectMapper();
    
    private String queueUrl;
    private String region;
    private String messageFormat;
    private List<String> fieldsToInclude;
    private boolean includeMetadata;
    private int delaySeconds;
    private String keyspaceName;
    private String tableName;
    
    public SQSTargetMapper(Config config) {
        super(config);
        this.keyspaceName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.keyspace-name", "", true);
        this.tableName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.table-name", "", true);
        this.queueUrl = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.queue-url", "", true);
        this.region = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.region", "us-east-1", false);
        this.messageFormat = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.message-format", "full", false);
        this.fieldsToInclude = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.fields-to-include", new ArrayList<String>(), false);
        this.includeMetadata = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.include-metadata", true, false);
        this.delaySeconds = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.delay-seconds", 0, false);
       
    }

    
    @Override
    public void initialize() {
        super.initialize();
        //once per application/connector, more performant to share across application, 
        //but may have different configuration per connector or connector instance.
        JSON_MAPPER = new ObjectMapper();
        JSON_MAPPER.registerModule(new JavaTimeModule());
    }

    
    @Override
    public void handleRecords(List<KeyspacesStreamsClientRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            logger.debug("No records to process");
            return;
        }
        
        List<SendMessageBatchRequestEntry> batchRequestEntries = convertRecordsToBatchRequestEntries(records);

        List<SendMessageBatchRequest> batchRequests = convertBatchEntriesToBatchRequest(batchRequestEntries);

        for(SendMessageBatchRequest batchRequest : batchRequests){
            sendBatchMessage(batchRequest);
        }

        logger.info("Successfully processed {} records to SQS queue: {}", records.size(), queueUrl);
    }
    protected List<SendMessageBatchRequestEntry> convertRecordsToBatchRequestEntries(List<KeyspacesStreamsClientRecord> records) throws Exception {
        ObjectNode oneMessageNode = JSON_MAPPER.createObjectNode();

        ArrayNode oneRecordsNode = JSON_MAPPER.createArrayNode();
        
        oneMessageNode.set("records", oneRecordsNode);

        logger.debug("Processing {} records for SQS queue: {}", records.size(), queueUrl);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        List<SendMessageBatchRequestEntry> batchRequestEntries = new ArrayList<>();

        int currentMessageSize = 0; //max size 1MB
        
        String lastSequenceNumber = "";

        for (KeyspacesStreamsClientRecord record : records) {
            
            lastSequenceNumber = record.sequenceNumber();

            ObjectNode oneJSONRecord = createMessageBody(record);

            JSON_MAPPER.writeValue(baos, oneJSONRecord);

            final int recordSize = baos.size();
                
            baos.reset();

            if((recordSize + currentMessageSize) < 1000 * 1024) {//1MB

                oneRecordsNode.add(oneJSONRecord);
                
                currentMessageSize += recordSize;
            }else{
                batchRequestEntries.add(
                    SendMessageBatchRequestEntry.builder()
                    .id(lastSequenceNumber + "-" + oneRecordsNode.size())
                    .messageBody(JSON_MAPPER.writeValueAsString(oneMessageNode))
                    .delaySeconds(delaySeconds).build());
                    
                oneMessageNode = JSON_MAPPER.createObjectNode();
                oneRecordsNode = JSON_MAPPER.createArrayNode();
                oneMessageNode.set("records", oneRecordsNode);
                oneRecordsNode.add(oneJSONRecord);
                
                currentMessageSize = recordSize;
            }
        }
        if(oneRecordsNode.size() > 0){
            batchRequestEntries.add(
                SendMessageBatchRequestEntry.builder()
                .id(lastSequenceNumber + "-" + oneRecordsNode.size())
                .messageBody(JSON_MAPPER.writeValueAsString(oneMessageNode))
                .delaySeconds(delaySeconds).build());
        }
        return batchRequestEntries;
    }

    protected ObjectNode createMessageBody(KeyspacesStreamsClientRecord record) throws Exception {
        ObjectNode messageNode = JSON_MAPPER.createObjectNode();
        
        // Add metadata if requested
        if (includeMetadata) {
            ObjectNode metadataNode = JSON_MAPPER.createObjectNode();
            metadataNode.put("keyspace", keyspaceName);
            metadataNode.put("table", tableName);
            metadataNode.put("operation", StreamHelpers.getOperationType(record.getRecord()).toString());
            metadataNode.put("timestamp", System.currentTimeMillis());
            metadataNode.put("sequenceNumber", record.sequenceNumber());
            messageNode.set("metadata", metadataNode);
        }
        
        // Add data based on message format
        switch (messageFormat.toLowerCase(Locale.ROOT)) {
            case "full":
                messageNode.set("newImage", extractFieldsAsJson(record.getRecord().newImage(), fieldsToInclude));
                messageNode.set("oldImage", extractFieldsAsJson(record.getRecord().oldImage(), fieldsToInclude));
                break;
                
            case "new-image":
                messageNode.set("newImage", extractFieldsAsJson(record.getRecord().newImage(), fieldsToInclude));
                break;
                
            case "old-image":
                messageNode.set("oldImage", extractFieldsAsJson(record.getRecord().oldImage(), fieldsToInclude));
                break;
                
            default:
                throw new IllegalArgumentException("Unsupported message format: " + messageFormat);
        }
        
        return messageNode;
    }
    protected List<SendMessageBatchRequest> convertBatchEntriesToBatchRequest(List<SendMessageBatchRequestEntry> entries) throws Exception {
        
        AtomicInteger counter = new AtomicInteger(0);
        
        return entries.stream()
        .collect(Collectors.groupingBy(s -> counter.getAndIncrement() / 10))
        .values().stream()
        .map(entryList -> SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(entryList)
                .build())
        .collect(Collectors.toList());
    
    }

    protected JsonNode extractFieldsAsJson(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow image, List<String> fields) {
        ObjectNode fieldsNode = JSON_MAPPER.createObjectNode();
        
        if (image != null && image.valueCells() != null) {
            for (String fieldName : fields) {
                if (image.valueCells().containsKey(fieldName)) {
                    Object fieldValue = StreamHelpers.getValueFromCell(image.valueCells().get(fieldName).value());
                    fieldsNode.putPOJO(fieldName, fieldValue);
                }
            }
        }
        
        return fieldsNode;
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
