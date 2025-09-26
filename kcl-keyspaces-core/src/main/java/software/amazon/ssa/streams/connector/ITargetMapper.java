package software.amazon.ssa.streams.connector;


import java.util.List;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;

public interface ITargetMapper {

    public void initialize();

    public List<KeyspacesStreamsClientRecord> filterRecords(List<KeyspacesStreamsClientRecord> records);

    public void handleRecords(List<KeyspacesStreamsClientRecord> records)  throws Exception;
}
