package software.amazon.ssa.streams.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;

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
public class JSONHelper {
    private static final Logger logger = LoggerFactory.getLogger(JSONHelper.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Converts a list of Keyspaces Streams records to JSON format.
     * 
     * This method creates a simplified JSON structure with the following format:
     * <pre>
     * {
     *   "records": [
     *     {
     *       "id": "user123",
     *       "name": "John Doe",
     *       "email": "john@example.com"
     *     },
     *     {
     *       "id": "user124",
     *       "name": "Jane Smith", 
     *       "email": "jane@example.com"
     *     }
     *   ]
     * }
     * </pre>
     * 
     * Each record in the array is the direct JSON object from the Keyspaces stream,
     * without any wrapper fields or metadata. This provides a clean, simple structure
     * that's easy to process downstream.
     * 
     * @param records List of Keyspaces Streams records to convert
     * @return byte array containing the JSON-formatted data in UTF-8 encoding
     * @throws Exception if JSON processing or record conversion fails
     */
    public static byte[] writeRecordsToJSON(List<KeyspacesStreamsClientRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            logger.warn("No records to write to JSON");
            return "{\"records\":[]}".getBytes(StandardCharsets.UTF_8);
        }

        // Create the root JSON object that will contain the records array
        ObjectNode payload = objectMapper.createObjectNode();
        
        // Note: Metadata fields are commented out but could be added if needed
        // This would include information like record count, sequence numbers, timestamps, etc.
        /*ObjectNode metadata = payload.putObject("metadata");
        metadata.put("recordCount", records.size());
        metadata.put("firstSequenceNumber", firstSequenceNumber);
        metadata.put("lastSequenceNumber", lastSequenceNumber);
        metadata.put("timestamp", timestamp.toString());
        metadata.put("base64Encoded", base64Encode);*/

        // Create the records array that will contain all the record objects
        ArrayNode recordsArray = payload.putArray("records");
        
        // Process each record in the input list
        for (KeyspacesStreamsClientRecord record : records) {
          
            // Extract the record data as a UTF-8 string
            // The record.data() returns a ByteBuffer containing the JSON data
            String jsonData = StandardCharsets.UTF_8.decode(record.data()).toString();
            
            // Parse the JSON string into a Jackson JsonNode and add it directly to the records array
            // This preserves the original JSON structure from the Keyspaces stream
            recordsArray.add(objectMapper.readTree(jsonData));
            
        }

        // Convert the complete JSON payload to a string
        String jsonPayload = objectMapper.writeValueAsString(payload);
        
        logger.info("Successfully converted {} records to JSON format ({} bytes)", records.size(), jsonPayload.length());
        
        // Return the JSON string as UTF-8 encoded bytes
        return jsonPayload.getBytes(StandardCharsets.UTF_8);
    }
}
