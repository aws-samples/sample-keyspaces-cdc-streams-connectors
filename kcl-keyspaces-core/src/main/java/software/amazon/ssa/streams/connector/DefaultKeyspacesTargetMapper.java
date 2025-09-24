package software.amazon.ssa.streams.connector;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.keyspaces.streamsadapter.serialization.RecordObjectMapper;
import software.amazon.ssa.streams.config.KeyspacesConfig;

public class DefaultKeyspacesTargetMapper implements ITargetMapper {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeyspacesTargetMapper.class);
    /***
     * Default implementation of the KeyspacesTargetMapper. 
     * This implementation will log the records.
     * 
     * @param records The list of records to be processed.  
     */
    private ObjectMapper mapper = new RecordObjectMapper();

    public DefaultKeyspacesTargetMapper(Config config) {
    }

    
    @Override
    public void initialize(KeyspacesConfig keyspacesConfig) {
        // TODO Auto-generated method stub
       // throw new UnsupportedOperationException("Unimplemented method 'initialize'");
    }

    @Override
    public void handleRecords(List<KeyspacesStreamsClientRecord> records) throws Exception {
        
        for (KeyspacesStreamsClientRecord record : records) {
          record.toString();
            String json = mapper.writeValueAsString(record);
            logger.info("Keyspaces CDC Record: {}", json);
       }
    }
}
