package software.amazon.ssa.streams.connector.s3;

import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayDeque;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.converters.IStreamRecordConverter;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.AbstractTargetMapper;

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
public class S3TargetMapper extends AbstractTargetMapper {

    private static final Logger logger = LoggerFactory.getLogger(S3TargetMapper.class);
    
    private S3Client s3Client;
    private String bucketName;
    private String prefix;
    private String regionName;
    private String outputFormat;
    private String timestampPartition;

    private int maxMessageSize;
    private int maxRecordsPerMessage;
    private String keyspaceName;
    private String tableName;

    private IStreamRecordConverter<Map<String, RequestBody>> jsonConverter;
    private IStreamRecordConverter<Map<String, RequestBody>> avroConverter;
   
    public S3TargetMapper(Config config) {
        super(config);
        this.bucketName = KeyspacesConfig.getConfigValue(config    , "keyspaces-cdc-streams.connector.bucket-id", "", true);
        this.prefix = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.prefix", "", false);
        this.regionName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.region", "us-east-1", true);
        this.outputFormat = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.output-format", "avro", false);
        this.timestampPartition = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.timestamp-partition", "hours", false);
       
        this.keyspaceName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.keyspace-name", "", false);
        this.tableName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.stream.table-name", "", false);
     }

    @Override
    public void initialize() {
        this.s3Client = S3Client.builder().region(Region.of(regionName)).build();
        this.jsonConverter = new S3JsonConverter(config);
        this.avroConverter = new S3AvroConverter(config);
    }

    @Override
    public void handleRecords(List<KeyspacesStreamsClientRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            logger.debug("No records to process");
            return;
        }

        StringBuilder time = new StringBuilder();
        
        Deque<String> dq = new ArrayDeque<String>();
              
        switch(timestampPartition) {
            case "seconds":
                dq.addFirst(String.format("%02d/", LocalDateTime.now().getSecond()));
            case "minutes":
                dq.addFirst(String.format("%02d/", LocalDateTime.now().getMinute()));
            case "hours":
                dq.addFirst(String.format("%02d/", LocalDateTime.now().getHour()));
            case "days":
                dq.addFirst(String.format("%02d/", LocalDateTime.now().getDayOfMonth()));
            case "months":
                dq.addFirst(String.format("%02d/", LocalDateTime.now().getMonthValue()));
            case "years":
                time.append(String.format("%04d/", LocalDateTime.now().getYear()));
                break;
            default:
                logger.info("No timestamp partition selected: " + timestampPartition);
        }

        StringBuilder partitionBuilder = new StringBuilder();
        dq.forEach(partitionBuilder::append);
        String partition = partitionBuilder.toString();

        Instant timestamp = Instant.now();

        // Create filename with sequence range and timestamp
       
        

        logger.info("Processing {} records for S3 upload", records.size());
        
       
        if(outputFormat.equalsIgnoreCase("avro")) {
            
            List<Map<String, RequestBody>> data = avroConverter.convertRecordsToMessages(records, keyspaceName, tableName);

            for(Map<String, RequestBody> oneBatch : data) {
                for(Map.Entry<String, RequestBody> entry : oneBatch.entrySet()) {
                    
                    String filename = String.format("%s-%d.%s", 
                    entry.getKey(),
                    timestamp.toEpochMilli(),
                    outputFormat.equalsIgnoreCase("avro") ? "avro" : "json");

                    String key = "";
                    if (partition.length() > 0) {
                        key = String.format("%s/%s/%s", prefix, partition, filename).replace("//", "/");
                    } else {
                        key = String.format("%s/%s", prefix, filename).replace("//", "/");
                    }

                    s3Client.putObject(
                        PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                            entry.getValue()
                        );
                }
            }
        } else if(outputFormat.equals("json")) {
            List<Map<String, RequestBody>> data = jsonConverter.convertRecordsToMessages(records, keyspaceName, tableName);
        
            for(Map<String, RequestBody> oneBatch : data) {
                for(Map.Entry<String, RequestBody> entry : oneBatch.entrySet()) {

                            String filename = String.format("%s-%d.%s", 
                            entry.getKey(),
                            timestamp.toEpochMilli(),
                            outputFormat.equalsIgnoreCase("avro") ? "avro" : "json");

                            String key = "";
                            if (partition.length() > 0) {
                                key = String.format("%s/%s/%s", prefix, partition, filename).replace("//", "/");
                            } else {
                                key = String.format("%s/%s", prefix, filename).replace("//", "/");
                            }

                            s3Client.putObject(
                                PutObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(key)
                                    .build(),
                                    entry.getValue()
                            );
                }
            }
        } else {
            throw new IllegalArgumentException("Invalid format: " + outputFormat);
        }

        logger.info("Successfully wrote {} records to S3", records.size());
    }
}
