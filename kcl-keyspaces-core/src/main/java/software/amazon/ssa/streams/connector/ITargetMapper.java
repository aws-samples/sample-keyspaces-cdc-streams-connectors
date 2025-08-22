package software.amazon.ssa.streams.connector;


import java.util.List;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.config.KeyspacesConfig;

public interface ITargetMapper {

    public void initialize(KeyspacesConfig keyspacesConfig);

    public void handleRecords(List<KeyspacesStreamsClientRecord> records)  throws Exception;
}
