package software.amazon.ssa.streams.converters;

import java.util.List;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;

/**
 * Interface for converting Keyspaces Streams records to different message formats.
 * 
 * This interface defines the contract for converting Keyspaces change data capture (CDC) records
 * into various message formats that can be consumed by different target systems.
 * 
 * @param <Message> The type of message that will be produced by the converter
 */
public interface IStreamRecordConverter<Message> {

    /**
     * Converts a single record to a message format.
     * 
     * @param id The identifier for the record
     * @param body The raw byte data of the record
     * @return A message of type Message containing the converted record
     * @throws Exception if conversion fails
     */
    Message convertRecordToMessage(String id, byte[] body) throws Exception;

    /**
     * Converts a list of Keyspaces Streams records to a list of messages.
     * 
     * This method handles batch processing of multiple records, applying
     * appropriate formatting and metadata inclusion based on the converter's configuration.
     * 
     * @param records List of Keyspaces Streams records to convert
     * @param keyspaceName The keyspace name for metadata purposes
     * @param tableName The table name for metadata purposes
     * @return List of messages of type Message containing the converted records
     * @throws Exception if conversion fails
     */
    List<Message> convertRecordsToMessages(List<KeyspacesStreamsClientRecord> records, String keyspaceName, String tableName) throws Exception;
}