package software.amazon.ssa.streams.connector.s3;

import java.util.Deque;
import java.util.List;

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
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.AbstractTargetMapper;
import software.amazon.ssa.streams.helpers.AvroHelper;
import software.amazon.ssa.streams.helpers.JSONHelper;

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
    private String format;
    private String timestampPartition;
    private int maxRetries;
   
    public S3TargetMapper(Config config) {
        super(config);
        this.bucketName = KeyspacesConfig.getConfigValue(config    , "keyspaces-cdc-streams.connector.bucket-id", "", true);
        this.prefix = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.prefix", "", false);
        this.regionName = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.region", "us-east-1", true);
        this.format = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.format", "avro", false);
        this.timestampPartition = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.timestamp-partition", "hours", false);
        this.maxRetries = KeyspacesConfig.getConfigValue( config, "keyspaces-cdc-streams.connector.max-retries", 3, false);
       
    }

    @Override
    public void initialize() {
        this.s3Client = S3Client.builder().region(Region.of(regionName)).build();
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

        String firstSequenceNumber = records.get(0).sequenceNumber();
        String lastSequenceNumber = records.get(records.size() - 1).sequenceNumber();
        Instant timestamp = Instant.now();

        // Create filename with sequence range and timestamp
        String filename = String.format("%s-%s-%d.%s", 
            firstSequenceNumber,
            lastSequenceNumber,
            timestamp.toEpochMilli(),
            format.equalsIgnoreCase("avro") ? "avro" : "json");
        
        String key = "";
        if (partition.length() > 0) {
            key = String.format("%s/%s/%s", prefix, partition, filename).replace("//", "/");
        } else {
            key = String.format("%s/%s", prefix, filename).replace("//", "/");
        }

        logger.info("Processing {} records for S3 upload", records.size());
        
        byte[] data;
        if(format.equalsIgnoreCase("avro")) {
            data = AvroHelper.writeRecordsToAvro(records);
        } else if(format.equals("json")) {
            data = JSONHelper.writeRecordsToJSON(records);
        } else {
            throw new IllegalArgumentException("Invalid format: " + format);
        }

        
        s3Client.putObject(
            PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(),
            RequestBody.fromBytes(data)
        );

        logger.info("Successfully wrote {} records to S3: {}", records.size(), key);
    }
}
