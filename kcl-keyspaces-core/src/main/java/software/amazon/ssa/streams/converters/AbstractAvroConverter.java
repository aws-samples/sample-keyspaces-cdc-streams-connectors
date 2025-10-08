package software.amazon.ssa.streams.converters;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.keyspaces.streamsadapter.serialization.RecordObjectMapper;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.helpers.StreamHelpers;
import software.amazon.ssa.streams.helpers.StreamHelpers.StreamProcessorOperationType;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Helper class for converting Keyspaces Streams records to Apache Avro format.
 * 
 * This class provides functionality to:
 * - Dynamically build Avro schemas based on Keyspaces table structure
 * - Convert Keyspaces Streams records to Avro format
 * - Map CQL data types to Avro schema types
 * - Handle different operation types (INSERT, UPDATE, DELETE, TTL)
 * 
 * The Avro format provides efficient serialization and schema evolution capabilities
 * for storing Keyspaces change data capture (CDC) records.
 */
public abstract class AbstractAvroConverter <Message> implements IStreamRecordConverter<Message> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAvroConverter.class);

    // Configuration parameters for record processing
    private String recordFormat;           // Format of records: "full", "new-image", "old-image", or "fields-only"
    private boolean includeMetadata;       // Whether to include CDC metadata in the output
    private List<String> fieldsToInclude; // Specific fields to include (used with fields-only format)
    private int maxMessageSize;           // Maximum size in bytes for each message
    private int maxRecordsPerMessage;     // Maximum number of records per message

    /**
     * Constructor that initializes the converter with configuration parameters.
     * 
     * @param config Configuration object containing converter settings
     */
    public AbstractAvroConverter(Config config){
        // Load configuration parameters with defaults
        this.recordFormat = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.record-format", "full", false);
        this.includeMetadata = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.include-metadata", true, false);
        this.fieldsToInclude = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.fields-to-include", new ArrayList<String>(), false);
        this.maxMessageSize = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.max-message-size", 256 * 1024, false);
        this.maxRecordsPerMessage = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.max-records-per-message", -1, false);
        
        // Register Avro logical type conversions for proper handling of complex data types
        GenericData.get().addLogicalTypeConversion(new org.apache.avro.Conversions.BigDecimalConversion());
        GenericData.get().addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
        GenericData.get().addLogicalTypeConversion(new org.apache.avro.Conversions.DurationConversion());
        GenericData.get().addLogicalTypeConversion(new org.apache.avro.Conversions.UUIDConversion());
        GenericData.get().addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
    }

    /**
     * Converts a single record to a message format.
     * This method is not implemented in the abstract class and must be overridden by concrete implementations.
     * 
     * @param id The identifier for the record
     * @param body The raw byte data of the record
     * @return A message of type Message containing the converted record
     * @throws Exception if conversion fails
     */
    @Override
    public Message convertRecordToMessage(String id, byte[] body) throws Exception {
        // This method must be implemented by concrete subclasses
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Determines whether old image data should be included in the output based on record format and operation type.
     * 
     * @param recordFormat The format of the record ("full", "new-image", "old-image", or "fields-only")
     * @param operationType The type of operation (INSERT, UPDATE, DELETE, TTL, etc.)
     * @return true if old image data should be included, false otherwise
     */
    public static boolean shouldIncludeOldImage(String recordFormat, StreamProcessorOperationType operationType){
        // Always include old image for "full" format
        if(recordFormat.equals("full")){
            return true;
        }
        // Include old image for DELETE operations regardless of format
        else if(operationType == StreamProcessorOperationType.DELETE || 
                operationType == StreamProcessorOperationType.REPLICATED_DELETE || 
                operationType == StreamProcessorOperationType.TTL){
            return true;
        }
        // Don't include old image for other operations in non-full formats
        else{
            return false;
        }
    }
    
    /**
     * Determines the field name for old image data based on record format.
     * 
     * @param fieldName The original field name
     * @param recordFormat The format of the record
     * @param operationType The type of operation (unused but kept for consistency)
     * @return The field name to use for old image data
     */
    public static String fieldNameForOldImage(String fieldName, String recordFormat, StreamProcessorOperationType operationType){
        // For "full" format, prefix old image fields with "old_"
        if(recordFormat.equals("full")){
            return "old_" + fieldName;
        }
        // For other formats, use the original field name
        else{
            return fieldName;
        }
    }
    /**
     * Converts a list of Keyspaces Streams records to a list of Avro messages.
     * This is the main conversion method that handles batch processing of multiple records.
     * 
     * @param records List of Keyspaces Streams records to convert
     * @param keyspaceName The keyspace name for metadata purposes
     * @param tableName The table name for metadata purposes
     * @return List of messages of type Message containing the converted records
     * @throws Exception if conversion fails
     */
    @Override
    public List<Message> convertRecordsToMessages(List<KeyspacesStreamsClientRecord> records, String keyspaceName, String tableName) throws Exception {
        
        // Step 1: Build the Avro schema based on the records and configuration
        Schema avroSchema = buildAvroSchema(records, includeMetadata, fieldsToInclude, recordFormat);
        
        // Step 2: Pre-process records into a unified map format for easier Avro conversion
        List<Map<String, Object>> recordsToConvert = records.stream()
         .map(rootRecord ->  rootRecord.getRecord()).map(record -> {
                // Determine the operation type for this record
                StreamProcessorOperationType operation_type =  StreamHelpers.getOperationType(record);

                // Create a map to hold all data for this record
                Map<String, Object> oneMap = new HashMap<>();
                
                // Add metadata fields if configured to include them
                oneMap.put("stream_keyspace_name", keyspaceName);
                oneMap.put("stream_table_name", tableName);
                oneMap.put("stream_operation_type", operation_type.toString());
                oneMap.put("stream_sequence_number", record.sequenceNumber());
                // Note: arrival timestamp is commented out but could be added if needed
                //oneMap.put("stream_arrival_timestamp", record.approximateArrivalTimestamp().toEpochMilli());
                
                // Process new image data (data after the change)
                Map<String, Object> newCells = Optional.ofNullable(record.newImage())
                    .map(img -> img.valueCells())
                    .orElseGet(Map::of)
                    .entrySet().stream()
                    .filter(entry -> fieldsToInclude == null || fieldsToInclude.isEmpty() || fieldsToInclude.contains(entry.getKey())) 
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> getValueFromCellOverride(entry)));

                // Process old image data (data before the change) based on format and operation type
                Map<String, Object> oldCells = Optional.ofNullable(record.oldImage())
                    .map(img -> img.valueCells())
                    .orElseGet(Map::of)
                    .entrySet().stream()
                    .filter(entry -> shouldIncludeOldImage(recordFormat, operation_type ))
                    .filter(entry -> fieldsToInclude == null || fieldsToInclude.isEmpty() || fieldsToInclude.contains(entry.getKey())) 
                    .collect(Collectors.toMap(
                        entry -> fieldNameForOldImage(entry.getKey(), recordFormat, operation_type),
                        entry -> getValueFromCellOverride(entry)));
                
                // Combine new and old image data into the final map
                oneMap.putAll(newCells);
                
                oneMap.putAll(oldCells);

                return oneMap;
            
            })
         .collect(Collectors.toList());
         

        // Step 3: Write records to Avro format using the generated schema
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream outForSizing = new ByteArrayOutputStream();

        // Create Avro writer components for efficient serialization
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        // Initialize Avro file writer with the generated schema
        dataFileWriter.create(avroSchema, outputStream);

        // Variables for tracking message batching
        int currentMessageSize = 0;
        int currentMessageRecordCount = 0;
        String firstRecordSequenceNumber =  records.get(0).sequenceNumber();
        String currentRecordSequenceNumber =   firstRecordSequenceNumber;

        List<Message> messageList = new ArrayList<>();

        // Step 4: Process each pre-converted record and create Avro messages
        for (Map<String, Object> oneMapRecord : recordsToConvert) {
            // Create JSON mapper for debugging purposes
            ObjectMapper mapper = new RecordObjectMapper();
            logger.debug("Record: {}", mapper.writeValueAsString(oneMapRecord));
          
            // Create a new Avro record using the generated schema
            GenericRecord avroRecord = new GenericData.Record(avroSchema);
           
            // Populate the Avro record with data from the pre-processed map
            for(Map.Entry<String, Object> entry : oneMapRecord.entrySet()){
                avroRecord.put(entry.getKey(), entry.getValue());
            }

            // Calculate the size of this record for batching decisions
            int size = getRecordSize(avroRecord, datumWriter, outForSizing);

            // Check if we need to start a new message due to size or record count limits
            if(size + currentMessageSize > maxMessageSize || 1 + currentMessageRecordCount > maxRecordsPerMessage){
                // Finalize the current message and add it to the batch
                dataFileWriter.close();
                
                messageList.add(convertRecordToMessage(firstRecordSequenceNumber + "-" + currentRecordSequenceNumber, outputStream.toByteArray()));
                
                // Reset for the next message
                outputStream = new ByteArrayOutputStream();
                dataFileWriter = new DataFileWriter<>(datumWriter);
                dataFileWriter.create(avroSchema, outputStream);

                currentMessageSize = 0;
                currentMessageRecordCount = 0;
                firstRecordSequenceNumber = oneMapRecord.get("stream_sequence_number").toString();
                currentRecordSequenceNumber = firstRecordSequenceNumber;
            }
            
            // Append the record to the current Avro file
            dataFileWriter.append(avroRecord);
            
            // Update tracking variables
            currentMessageSize += size;
            currentMessageRecordCount++;
            currentRecordSequenceNumber = oneMapRecord.get("stream_sequence_number").toString();
            
        }
        
        // Step 5: Finalize the last message if it contains any records
        dataFileWriter.close();
        if(currentMessageRecordCount > 0 || currentMessageSize > 0){
            messageList.add(convertRecordToMessage(firstRecordSequenceNumber + "-" + currentRecordSequenceNumber, outputStream.toByteArray()));
        }
        
        return messageList;
        
    }
    /**
     * Override method to handle Avro limitations with BigDecimal and other complex data types.
     * This method converts Keyspaces cell values to Java objects that are compatible with Avro serialization.
     * 
     * @param cell The Keyspaces cell containing the value to convert
     * @return The converted value suitable for Avro serialization
     */
    public Object getValueFromCellOverride(Map.Entry<String, KeyspacesCell> cell) {
        KeyspacesCellValue cellValue = cell.getValue().value();
        
        // Get the CQL type name for special handling
        String cqlType = cellValue.type().name().toLowerCase();

        // Get the base value using the standard helper method
        Object value = StreamHelpers.getValueFromCell(cellValue);
        
        // Handle special cases for Avro compatibility
        switch (cqlType) {
            case "decimalt":
                // Convert BigDecimal to string to avoid Avro serialization issues
                return value.toString();
            default:
                // Return the value as-is for other types
                return value;
        }
    }
    
    /**
     * Calculates the serialized size of an Avro record for batching decisions.
     * This method serializes the record to determine its byte size without affecting the main output stream.
     * 
     * @param record The Avro record to measure
     * @param writer The Avro datum writer for serialization
     * @param outForSizing A temporary output stream for size calculation
     * @return The size in bytes of the serialized record
     * @throws Exception if serialization fails
     */
    public int getRecordSize(GenericRecord record, DatumWriter<GenericRecord> writer, ByteArrayOutputStream outForSizing) throws Exception {
        
        // Create a binary encoder for size calculation
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outForSizing, null);
        
        // Write the record to the sizing stream
        writer.write(record, encoder);
        
        // Flush the encoder to ensure all data is written
        encoder.flush();
        
        // Get the size of the serialized data
        int size = outForSizing.toByteArray().length;
        
        // Reset the stream for reuse
        outForSizing.reset();

        return size;
    }

    /**
     * Dynamically builds an Avro schema based on the structure of Keyspaces records.
     * 
     * The schema is built by analyzing the first available record that contains either
     * a newImage (for INSERT/UPDATE operations) or oldImage (for DELETE/TTL operations).
     * Each column in the Keyspaces table becomes a field in the Avro schema.
     * 
     * @param sampleRecords List of records to analyze for schema building
     * @return Avro schema representing the Keyspaces table structure
     * @throws Exception if no suitable sample record is found or schema building fails
     */
    private static Schema buildAvroSchema(List<KeyspacesStreamsClientRecord> sampleRecords, boolean includeMetadata, List<String> fieldsToInclude, String recordFormat) throws Exception {
        // Create a new Avro record schema with namespace
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("keyspaces_record")
            .namespace("software.amazon.ssa.streams.connector.target.s3");
        
        SchemaBuilder.FieldAssembler<Schema> fields = recordBuilder.fields();
        
        // Note: Metadata fields are commented out but could be added if needed
        //fields.name("record_id").type().stringType().noDefault();
        //fields.name("stream_arn").type().stringType().noDefault();
        //fields.name("sequence_number").type().stringType().noDefault();
        //fields.name("operation").type().stringType().noDefault();
        //fields.name("keyspace_name").type().stringType().noDefault();
        if(includeMetadata){
            fields.name("stream_table_name").type().stringType().noDefault();
            fields.name("stream_keyspace_name").type().stringType().noDefault();
            fields.name("stream_sequence_number").type().stringType().noDefault();
            fields.name("stream_operation_type").type().stringType().noDefault();
        }

        KeyspacesRow row = null;

        // Find a sample record with either newImage or oldImage to build schema
        // Prefer newImage (INSERT/UPDATE) over oldImage (DELETE/TTL) for schema building
        KeyspacesStreamsClientRecord sampleRecord = sampleRecords.stream()
            .filter(x -> x.getRecord().newImage() != null)
            .findAny()
            .orElse(null);

        if(sampleRecord != null){
            // Use newImage for schema building (INSERT/UPDATE operations)
            row = sampleRecord.getRecord().newImage();
        } else {
            // Fallback to oldImage if no newImage records exist (DELETE/TTL operations)
            sampleRecord = sampleRecords.stream()
                .filter(x -> x.getRecord().oldImage() != null)
                .findAny()
                .orElseThrow(() -> new Exception("No sample record with newImage or oldImage found"));
            row = sampleRecord.getRecord().oldImage();
        }

        if(recordFormat.equals("full")){
            addFieldsToSchema("", fields, row, fieldsToInclude);
            addFieldsToSchema("old_", fields, row, fieldsToInclude);
        }else{
            addFieldsToSchema("", fields, row, fieldsToInclude);
        }
        
        // Add fields to Avro schema based on Keyspaces table columns
       

        // Add operation type field to track the type of change
        //fields.name("aks_operation_type").type(Schema.create(Schema.Type.STRING)).noDefault();
        
        return fields.endRecord();
    }

    /**
     * Maps CQL (Cassandra Query Language) data types to Avro schema types.
     * 
     * This method converts Keyspaces/Cassandra data types to their corresponding
     * Avro schema types for proper serialization.
     * 
     * @param cqlType The CQL data type from Keyspaces
     * @param fieldName The name of the field (for error reporting)
     * @return Avro schema type corresponding to the CQL type
     * @throws IllegalArgumentException if the CQL type is not supported
     */
    private static Schema mapCqlTypeToAvroSchema(Type cqlType) {

        String cqlTypeName = cqlType.name().toLowerCase();

        Schema schema = null;
        
        switch (cqlTypeName) {
            // String types
            case "textt":
            case "varchart":
            case "asciit":
            case "inett":
            case "decimalt": 
                schema = Schema.create(Schema.Type.STRING);
                break;
            case "datet":
                Schema dateInt = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                Schema union   = Schema.createUnion(Arrays.asList(
                    Schema.create(Schema.Type.NULL),
                    dateInt
                ));
                return union;
            // Integer types
            case "intt":
            case "smallintt":
            case "tinyintt":
                schema = Schema.create(Schema.Type.INT);
                break;
            // Long integer types
            case "bigintt":
            case "countert":
                schema = Schema.create(Schema.Type.LONG);
                break;
            // Floating point types
            case "floatt":
                schema = Schema.create(Schema.Type.FLOAT);
                break;
            case "doublet":
                schema = Schema.create(Schema.Type.DOUBLE);
                break;

            // Boolean type
            case "booleant":
                schema = Schema.create(Schema.Type.BOOLEAN);
                break;
            case "timestampt":
                schema = Schema.create(Schema.Type.LONG);
                break;
            // Binary data type
            case "blobt":
                schema = Schema.create(Schema.Type.BYTES);
                break;
            default:
                // Default to string for unknown types
                throw new IllegalArgumentException("Unsupported CQL type: " + cqlType);
        }

        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }

    /**
     * Determines the operation type for a given Keyspaces Streams record.
     * This method is available for use by subclasses or future implementations.
     * 
     * @param record The Keyspaces Streams record to analyze
     * @return The operation type (INSERT, UPDATE, DELETE, TTL, etc.)
     * @throws Exception if operation type cannot be determined
     */
    @SuppressWarnings("unused")
    private static StreamProcessorOperationType getOperationType(KeyspacesStreamsClientRecord record) throws Exception {
        return StreamHelpers.getOperationType(record.getRecord());
    }
    
    /**
     * Adds fields from a Keyspaces row to the Avro schema.
     * This method iterates through all cells in a Keyspaces row and adds corresponding
     * fields to the Avro schema with appropriate data types.
     * 
     * @param prefix Prefix to add to field names (e.g., "old_" for old image fields)
     * @param fields The Avro schema field assembler to add fields to
     * @param row The Keyspaces row containing the data structure
     * @param fieldsToInclude Optional list of specific fields to include (null means include all)
     * @throws Exception if schema building fails
     */
    private static void addFieldsToSchema(String prefix, SchemaBuilder.FieldAssembler<Schema> fields, 
                                        KeyspacesRow row, 
                                        List<String> fieldsToInclude) throws Exception {
        // Iterate through all cells in the Keyspaces row
        for (Map.Entry<String, KeyspacesCell> cell : row.valueCells().entrySet()) {
            String fieldName = cell.getKey();

            // Check if we should include this field based on the fieldsToInclude filter
            if(fieldsToInclude != null && fieldsToInclude.size() > 0){
                // Only include fields that are in the fieldsToInclude list
                if(fieldsToInclude.contains(fieldName)){
                    String schemaName = prefix + fieldName;
                    Type cellType = cell.getValue().value().type();
                    Schema fieldSchema = mapCqlTypeToAvroSchema(cellType);
                    fields.name(schemaName).type(fieldSchema).noDefault();
                }
            }else{
                // Include all fields if no filter is specified
                String schemaName = prefix + fieldName;
                Type cellType = cell.getValue().value().type();
                Schema fieldSchema = mapCqlTypeToAvroSchema(cellType);
                fields.name(schemaName).type(fieldSchema).noDefault();
            }
        }
    }
    
}
