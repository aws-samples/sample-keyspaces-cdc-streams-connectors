package software.amazon.ssa.streams.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.helpers.StreamHelpers;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Helper class for converting Keyspaces Streams records to JSON format.
 * 
 * This class provides functionality to:
 * - Convert Keyspaces Streams records to a simplified JSON structure
 * - Create a clean JSON payload with records array
 * - Handle record processing errors gracefully
 * - Maintain sequence integrity even when individual records fail
 * 
 * The JSON format provides a simple, human-readable structure for storing
 * Keyspaces change data capture (CDC) records without complex metadata wrappers.
 */
public class AbstractJSONConverter<Message> implements IStreamRecordConverter<Message>{
    private static final Logger logger = LoggerFactory.getLogger(AbstractJSONConverter.class);
    
    private static final ObjectMapper JSON_MAPPER = createJSONMapper();
    
    private List<String> fieldsToInclude;
    private boolean includeMetadata;
    private String recordFormat;
    private int maxMessageSize;
    private int maxRecordsPerMessage;

    public AbstractJSONConverter(Config config){
        this.fieldsToInclude = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.connector.fields-to-include", new ArrayList<String>(), false);
        this.includeMetadata = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.connector.include-metadata", true, false);
        this.recordFormat = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.connector.record-format", "full", false);
        this.maxMessageSize = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.connector.max-message-size", 256 * 1024, false);
        this.maxRecordsPerMessage = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.connector.max-records-per-message", -1, false);
    }
    
    @SuppressWarnings("MS_EXPOSE_REP")
    public static ObjectMapper getOrCreateJSONMapper() {
        return JSON_MAPPER;
    }

    private static ObjectMapper createJSONMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    
    
    public Message convertRecordToMessage(String id, byte[] record) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
        //currentBatch.put(firstRecordSequenceNumber + "-" + currentRecordSequenceNumber, currentMessage);
       // return convertRecordToMessage(record, messageFormat, includeMetadata, fieldsToInclude, keyspaceName, tableName);
    }

    /**
     * Converts a list of Keyspaces Streams records to a list of ArrayNodes with configurable options.
     * 
     * This method creates batched JSON structures that can be easily transformed into different SDKs.
     * The structure is: Multiple records per message, multiple messages per batch.
     * 
     * Each ArrayNode in the returned list represents a batch of messages, where each message
     * contains multiple records that fit within the specified constraints.
     * 
     * @param records List of Keyspaces Streams records to convert
     * @param recordFormat Message format - "full", "new-image", "old-image", or "fields-only"
     * @param includeMetadata Whether to include CDC metadata in messages
     * @param fieldsToInclude List of fields to include (used with fields-only format)
     * @param keyspaceName Keyspace name for metadata
     * @param tableName Table name for metadata
     * @param maxMessageSize Maximum size in bytes for each message (-1 for no limit)
     * @param maxRecordsPerMessage Maximum number of records per message (-1 for no limit)
     * @param maxBatchSize Maximum size in bytes for each batch (-1 for no limit)
     * @param maxMessagesPerBatch Maximum number of messages per batch (-1 for no limit)
     * @return Map of key, and List of ArrayNodes, each containing a batch of messages (each message contains records)
     * @throws Exception if JSON processing or record conversion fails
     */
    public List<Message> convertRecordsToMessages(
            List<KeyspacesStreamsClientRecord> records,
            String keyspaceName,
            String tableName) throws Exception {
        
        if (records == null || records.isEmpty()) {
            logger.warn("No records to convert to batched ArrayNodes");
            return new ArrayList<>();
        }

        ObjectMapper objectMapper = getOrCreateJSONMapper();
        
        logger.debug("Converting {} records to batched ArrayNodes with format: {}, includeMetadata: {}", 
                    records.size(), recordFormat, includeMetadata);
 
        List<Message> currentBatch = new ArrayList<>();
        
        ArrayNode currentMessage = objectMapper.createArrayNode();
        
        int currentMessageSize = 0;
        int currentMessageRecordCount = 0;
        int currentBatchMessageCount = 0;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        String firstRecordSequenceNumber = "0";
        String currentRecordSequenceNumber =  "99999";

        if(records.size() > 0 ){
            firstRecordSequenceNumber = records.get(0).sequenceNumber();
            currentRecordSequenceNumber = firstRecordSequenceNumber;
        }

        for (KeyspacesStreamsClientRecord record : records) {
            // Create the JSON representation of the record
            ObjectNode recordNode = createRecordNode(objectMapper, record, recordFormat, includeMetadata, 
                                                   fieldsToInclude, keyspaceName, tableName);
            
            // Calculate the size of this record
            baos.reset();
            objectMapper.writeValue(baos, recordNode);
            int recordSize = baos.size();
            
            // Check if we need to start a new message
            boolean needNewMessage = false;
            
            // Check message size limit
            if (maxMessageSize != -1 && (currentMessageSize + recordSize > maxMessageSize)) {
                needNewMessage = true;
            }
            
            // Check message record count limit
            if (maxRecordsPerMessage != -1 && currentMessageRecordCount >= maxRecordsPerMessage) {
                needNewMessage = true;
            }
            
            if (needNewMessage && currentMessageRecordCount > 0) {
                byte[] currentMessageBytes =  objectMapper.writeValueAsBytes(currentMessage);
                // Finalize current message and add to batch
                currentBatch.add(convertRecordToMessage(firstRecordSequenceNumber + "-" + currentRecordSequenceNumber, currentMessageBytes));
                currentBatchMessageCount++;
                
                // Start new message
                currentMessage = objectMapper.createArrayNode();
                currentMessageSize = 0;
                currentMessageRecordCount = 0;
                firstRecordSequenceNumber = record.sequenceNumber();
                currentRecordSequenceNumber = firstRecordSequenceNumber;
            }
            
            currentRecordSequenceNumber = record.sequenceNumber(); 
            
           
            // Add the record to the current message
            currentMessage.add(recordNode);
            currentMessageRecordCount++;
            currentMessageSize += recordSize;
        }
        
        // Add the last message if it has records
        if (currentMessageRecordCount > 0) {
            byte[] currentMessageBytes =  objectMapper.writeValueAsBytes(currentMessage);
            currentBatch.add(convertRecordToMessage(firstRecordSequenceNumber + "-" + currentRecordSequenceNumber + "-" + System.currentTimeMillis(), currentMessageBytes));
            currentBatchMessageCount++;
        }
        
        logger.info("Successfully converted {} records into {} batches with {} total messages", 
                   records.size(), currentBatch.size(), currentBatchMessageCount);

        return currentBatch;
    }

    /**
     * Creates a JSON ObjectNode for a single KeyspacesStreamsClientRecord.
     * 
     * @param record The KeyspacesStreamsClientRecord to convert
     * @param recordFormat Message format - "full", "new-image", "old-image", or "fields-only"
     * @param includeMetadata Whether to include CDC metadata
     * @param fieldsToInclude List of fields to include (used with fields-only format)
     * @param keyspaceName Keyspace name for metadata
     * @param tableName Table name for metadata
     * @return ObjectNode representing the record
     * @throws Exception if JSON processing fails
     */
    protected static ObjectNode createRecordNode(ObjectMapper JSONMapper, KeyspacesStreamsClientRecord record,
                                             String recordFormat,
                                             boolean includeMetadata,
                                             List<String> fieldsToInclude,
                                             String keyspaceName,
                                             String tableName) throws Exception {
        
        ObjectNode recordNode = JSONMapper.createObjectNode();
        
        // Add metadata if requested
        if (includeMetadata) {
            ObjectNode metadataNode = JSONMapper.createObjectNode();
            metadataNode.put("stream_keyspace_name", keyspaceName);
            metadataNode.put("stream_table_name", tableName);
            metadataNode.put("stream_operation_type", StreamHelpers.getOperationType(record.getRecord()).toString());
            metadataNode.put("stream_arrival_timestamp", record.approximateArrivalTimestamp().toEpochMilli());
            metadataNode.put("stream_sequence_number", record.sequenceNumber());
            recordNode.set("metadata", metadataNode);
        }
        
        // Add data based on message format
        switch (recordFormat.toLowerCase(Locale.ROOT)) {
            case "full":
                recordNode.set("image", extractFieldsAsJson(JSONMapper, record.getRecord().newImage(), fieldsToInclude));
                recordNode.set("oldImage", extractFieldsAsJson(JSONMapper, record.getRecord().oldImage(), fieldsToInclude));
                break;
                
            default:
                if(record.getRecord().newImage() != null){
                    recordNode.set("image", extractFieldsAsJson(JSONMapper, record.getRecord().newImage(), fieldsToInclude));
                }else{
                    recordNode.set("image", extractFieldsAsJson(JSONMapper, record.getRecord().oldImage(), fieldsToInclude));
                }
                break;
        }
        
        return recordNode;
    }

    /**
     * Extracts specified fields from a KeyspacesRow and returns them as a JsonNode.
     * 
     * @param image The KeyspacesRow to extract fields from
     * @param fields List of field names to extract (null means extract all)
     * @return JsonNode containing the extracted fields
     */
    protected static ObjectNode extractFieldsAsJson(ObjectMapper JSONMapper, software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow image, 
                                                List<String> fields) 
                                                {
        ObjectNode fieldsNode = JSONMapper.createObjectNode();
        
        if (image != null && image.valueCells() != null) {
            if (fields == null || fields.isEmpty()) {
                // Extract all fields
                for (Map.Entry<String, software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell> entry : image.valueCells().entrySet()) {
                    Object fieldValue = StreamHelpers.getValueFromCell(entry.getValue().value());
                    fieldsNode.putPOJO(entry.getKey(), fieldValue);
                }
            } else {
                // Extract only specified fields
                for (String fieldName : fields) {
                    if (image.valueCells().containsKey(fieldName)) {
                        Object fieldValue = StreamHelpers.getValueFromCell(image.valueCells().get(fieldName).value());
                        fieldsNode.putPOJO(fieldName, fieldValue);
                    }
                }
            }
        }
        
        return fieldsNode;
    }
}
