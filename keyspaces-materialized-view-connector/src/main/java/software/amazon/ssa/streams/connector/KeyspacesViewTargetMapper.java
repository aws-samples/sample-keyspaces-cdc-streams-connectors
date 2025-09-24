package software.amazon.ssa.streams.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.typesafe.config.Config;

import java.util.ArrayList;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.helpers.StreamHelpers;
import software.amazon.ssa.streams.helpers.StreamHelpers.StreamProcessorOperationType;

/**
 * S3 Target Mapper for Amazon Keyspaces CDC Streams
 * 
 * This connector writes Keyspaces CDC records to Amazon S3 in either Avro or JSON format.
 * It supports time-based partitioning and configurable retry logic for reliable data delivery.
 * 
 * Configuration:
 * - bucket-id: S3 bucket name (required)
 * - prefix: S3 key prefix for organizing files (optional)
 * - region: AWS region (default: us-east-1)
 * - format: Output format - "avro" or "json" (default: avro)
 * - timestamp-partition: Time partitioning granularity (default: hours)
 * - max-retries: Maximum retry attempts for S3 operations (default: 3)
 */
public class KeyspacesViewTargetMapper implements ITargetMapper {

    private static final Logger logger = LoggerFactory.getLogger(KeyspacesViewTargetMapper.class);
    
    private CqlSession session;
    private String keyspaceName;
    private String tableName;
    private String driverConfig;
    private List<String> fieldsToInclude;
    private int maxRetries;
    private List<String> partitionKeys;
    private List<String> clusteringKeys;
    private KeyspacesConfig keyspacesConfig;
   
    public KeyspacesViewTargetMapper(Config config) {
        this.keyspaceName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.keyspace-name", "", true);
        this.tableName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.table-name", "", true);
        this.maxRetries = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.max-retries", 3, false);
        this.fieldsToInclude = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.fields-to-include", new ArrayList<String>(), true);
        this.partitionKeys = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.partition-keys", new ArrayList<String>(), true);
        this.clusteringKeys = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.clustering-keys", new ArrayList<String>(), true);
        this.driverConfig = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.driver-config", "keyspaces-application.conf", true);
    }

    public synchronized CqlSession getOrCreateSession() {
        if (session == null) {
            DriverConfigLoader driverConfigLoader = DriverConfigLoader.fromClasspath(driverConfig);
            this.session = CqlSession.builder()
                .withConfigLoader(driverConfigLoader)
                .build();
        }
        return session;
    }
    @Override
    public void initialize(KeyspacesConfig keyspacesConfig) {
        this.keyspacesConfig = keyspacesConfig;
        
        DriverConfigLoader driverConfigLoader = DriverConfigLoader.fromClasspath(driverConfig);
        this.session = CqlSession.builder()
            .withConfigLoader(driverConfigLoader)
            .build();
        
        logger.info("Initializing Keyspaces materialized view connector with bucket: {} and prefix: {}", keyspaceName, tableName);
    }

    @Override
    public void handleRecords(List<KeyspacesStreamsClientRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            logger.debug("No records to process");
            return;
        }

       
        String insertStatement = new StringBuilder("INSERT INTO ")
        .append(keyspaceName)
        .append(".")
        .append(tableName)
        .append(" (")
        .append(String.join(", ", fieldsToInclude))
        .append(") VALUES (")
        .append(String.join(", ", fieldsToInclude.stream().map(field -> ":" + field).collect(Collectors.toList())))
        .append(")")
        .toString();

        String deleteStatement = new StringBuilder("DELETE FROM ")
        .append(keyspaceName)
        .append(".")
        .append(tableName)
        .append(" WHERE ")
        .append(String.join(" AND ", partitionKeys.stream().map(key -> key + " = :" + key).collect(Collectors.toList())))
        .append(" AND ")
        .append(String.join(" AND ", clusteringKeys.stream().map(key -> key + " = :" + key).collect(Collectors.toList())))
        .toString();

        String statement = insertStatement;

        Map<String, Object> values = new HashMap<String, Object>();
        
        for (KeyspacesStreamsClientRecord record : records) {

            StreamProcessorOperationType operationType = StreamHelpers.getOperationType(record.getRecord());

            if(operationType == StreamProcessorOperationType.INSERT || operationType == StreamProcessorOperationType.UPDATE || operationType == StreamProcessorOperationType.REPLICATED_INSERT || operationType == StreamProcessorOperationType.REPLICATED_UPDATE){
                for (String field : fieldsToInclude) {
                    values.put(field, StreamHelpers.getValueFromCell(record.getRecord().newImage().valueCells().get(field).value()));
                }
                statement = insertStatement;
            }else if(operationType == StreamProcessorOperationType.DELETE || operationType == StreamProcessorOperationType.TTL || operationType == StreamProcessorOperationType.REPLICATED_DELETE){
                for (String key : partitionKeys) {
                    values.put(key, StreamHelpers.getValueFromCell(record.getRecord().oldImage().valueCells().get(key).value()));
                }
                for (String key : clusteringKeys) {
                    values.put(key, StreamHelpers.getValueFromCell(record.getRecord().oldImage().valueCells().get(key).value()));
                }
                statement = deleteStatement;
            }else{
               throw new Exception("Unsupported operation type: " + operationType);
            }

            
            boolean success = false;
            for (int attempt = 0; attempt < maxRetries && !success; attempt++) {
                try {
                    getOrCreateSession().execute(statement, values);
                    success = true;
                    logger.info("Successfully wrote {} records to Keyspaces: {}", records.size(), statement);
                } catch (Exception error) {
                    logger.error("Failed write attempt to Keysapces {} failed: {}", attempt, error.getMessage());
                    if (attempt < maxRetries-1) {
                        Thread.sleep(10 * attempt); // Exponential backoff
                    } else {
                        throw error;
                    }
                }
            }
            values.clear();
        }
        
        
    }
}
