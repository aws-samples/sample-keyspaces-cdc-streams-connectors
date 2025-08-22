package software.amazon.ssa.streams.helpers;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.keyspaces.streamsadapter.serialization.RecordObjectMapper;
import software.amazon.ssa.streams.helpers.StreamHelpers.StreamProcessorOperationType;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

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
public class AvroHelper {
    private static final Logger logger = LoggerFactory.getLogger(AvroHelper.class);

    /**
     * Converts a list of Keyspaces Streams records to Avro format.
     * 
     * This method performs a two-step process:
     * 1. Dynamically builds an Avro schema based on the first record's structure
     * 2. Serializes all records to Avro format using the generated schema
     * 
     * @param records List of Keyspaces Streams records to convert
     * @return byte array containing the Avro-formatted data
     * @throws Exception if schema building or record processing fails
     * @throws IOException if Avro serialization fails
     */
    public static byte[] writeRecordsToAvro(List<KeyspacesStreamsClientRecord> records) throws Exception, IOException {
        // Step 1: Build Avro schema dynamically from the first record
        Schema avroSchema = buildAvroSchema(records);
        
        // Step 2: Write records to Avro format using the generated schema
        byte[] avroData = writeRecordsToAvro(records, avroSchema);
        
        return avroData;
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
    private static Schema buildAvroSchema(List<KeyspacesStreamsClientRecord> sampleRecords) throws Exception {
        // Create a new Avro record schema with namespace
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("keyspaces_record")
            .namespace("software.amazon.ssa.streams.connector.target.s3");
        
        SchemaBuilder.FieldAssembler<Schema> fields = recordBuilder.fields();
        
        // Note: Metadata fields are commented out but could be added if needed
        //fields.name("record_id").type().stringType().noDefault();
        //fields.name("stream_arn").type().stringType().noDefault();
        //fields.name("shard_id").type().stringType().noDefault();
        //fields.name("sequence_number").type().stringType().noDefault();
        //fields.name("operation").type().stringType().noDefault();
        //fields.name("keyspace_name").type().stringType().noDefault();
        //fields.name("table_name").type().stringType().noDefault();
        //fields.name("timestamp").type().longType().noDefault();

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

        // Add fields to Avro schema based on Keyspaces table columns
        for (Map.Entry<String, KeyspacesCell> cell : row.valueCells().entrySet()) {
            String fieldName = cell.getKey();
            Type cellType = cell.getValue().value().type();
            Schema fieldSchema = mapCqlTypeToAvroSchema(cellType, fieldName);
            fields.name(fieldName).type(fieldSchema).noDefault();
        }

        // Add operation type field to track the type of change
        fields.name("aks_operation_type").type(Schema.create(Schema.Type.STRING)).noDefault();
        
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
    private static Schema mapCqlTypeToAvroSchema(Type cqlType, String fieldName) {
        String cqlTypeName = cqlType.name().toLowerCase();
        
        switch (cqlTypeName) {
            // String types
            case "textt":
            case "varchart":
            case "asciit":
            case "inett":
                return Schema.create(Schema.Type.STRING);
                
            // Integer types
            case "intt":
            case "smallintt":
            case "tinyintt":
                return Schema.create(Schema.Type.INT);
                
            // Long integer types
            case "bigintt":
            case "countert":
                return Schema.create(Schema.Type.LONG);
                
            // Floating point types
            case "floatt":
                return Schema.create(Schema.Type.FLOAT);
                
            case "doublet":
                return Schema.create(Schema.Type.DOUBLE);
                
            // Boolean type
            case "booleant":
                return Schema.create(Schema.Type.BOOLEAN);
                
            // Timestamp type (stored as long)
            case "timestampt":
                return Schema.create(Schema.Type.LONG);
                
            // Binary data type
            case "blobt":
                return Schema.create(Schema.Type.BYTES);
                
            default:
                // Default to string for unknown types
                throw new IllegalArgumentException("Unsupported CQL type: " + cqlType);
        }
    }

    /**
     * Determines the operation type for a given Keyspaces Streams record.
     * 
     * @param record The Keyspaces Streams record to analyze
     * @return The operation type (INSERT, UPDATE, DELETE, TTL, etc.)
     * @throws Exception if operation type cannot be determined
     */
    private static StreamProcessorOperationType getOperationType(KeyspacesStreamsClientRecord record) throws Exception {
        return StreamHelpers.getOperationType(record.getRecord());
    }
    

    /**
     * Serializes Keyspaces Streams records to Avro format using the provided schema.
     * 
     * This method creates an Avro DataFileWriter and processes each record:
     * - Extracts field values based on operation type (newImage vs oldImage)
     * - Creates GenericRecord instances with the extracted data
     * - Writes records to the Avro output stream
     * 
     * @param records List of Keyspaces Streams records to serialize
     * @param schema Avro schema to use for serialization
     * @return byte array containing the Avro-formatted data
     * @throws Exception if record processing fails
     * @throws IOException if Avro serialization fails
     */
    private static byte[] writeRecordsToAvro(List<KeyspacesStreamsClientRecord> records, Schema schema) throws Exception, IOException {
        // Create output stream for Avro data
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // Create Avro writer components
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        // Initialize Avro file writer with schema
        dataFileWriter.create(schema, outputStream);

        // Process each record
        for (KeyspacesStreamsClientRecord record : records) {
            // Create JSON mapper for debugging
            ObjectMapper mapper = new RecordObjectMapper();
            logger.debug("Record: {}", mapper.writeValueAsString(record));
          
            // Determine operation type for this record
            StreamProcessorOperationType operation_type = getOperationType(record);

            // Create new Avro record
            GenericRecord avroRecord = new GenericData.Record(schema);

            // Add operation type to record
            avroRecord.put("aks_operation_type", operation_type.toString());

            // Handle different operation types
            if(operation_type == StreamProcessorOperationType.DELETE || 
               operation_type == StreamProcessorOperationType.REPLICATED_DELETE || 
               operation_type == StreamProcessorOperationType.TTL){
                
                // For DELETE operations, use oldImage (data before deletion)
                for (Map.Entry<String, KeyspacesCell> cell : record.getRecord().oldImage().valueCells().entrySet()) {
                    String fieldName = cell.getKey();
                    Object fieldValue = StreamHelpers.getValueFromCell(cell);
                    avroRecord.put(fieldName, fieldValue);
                }
                
            } else {
                // For INSERT/UPDATE operations, use newImage (data after change)
                for (Map.Entry<String, KeyspacesCell> cell : record.getRecord().newImage().valueCells().entrySet()) {
                    String fieldName = cell.getKey();
                    Object fieldValue = StreamHelpers.getValueFromCell(cell);
                    avroRecord.put(fieldName, fieldValue);
                }
            }
            
            // Append record to Avro file
            dataFileWriter.append(avroRecord);
        }

        // Close writer and return byte array
        dataFileWriter.close();
        return outputStream.toByteArray();
    }
}
