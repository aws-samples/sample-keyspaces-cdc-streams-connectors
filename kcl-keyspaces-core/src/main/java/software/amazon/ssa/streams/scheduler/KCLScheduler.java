package software.amazon.ssa.streams.scheduler;

import software.amazon.keyspaces.streamsadapter.AmazonKeyspacesStreamsAdapterClient;
import software.amazon.keyspaces.streamsadapter.StreamsSchedulerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.keyspacesstreams.KeyspacesStreamsClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.ssa.streams.processor.KeyspacesRecordProcessorFactory;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.ITargetMapper;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KCLScheduler {

    private static final Logger logger = LoggerFactory.getLogger(KCLScheduler.class);
    
    protected KeyspacesStreamsClient streamsClient;
    
    protected KinesisAsyncClient adapterClient;
    protected DynamoDbAsyncClient dynamoDbAsyncClient;
    protected CloudWatchAsyncClient cloudWatchClient;
    protected KeyspacesConfig keyspacesConfig;
    protected KeyspacesRecordProcessorFactory recordProcessorFactory;
    protected Scheduler scheduler;
    protected Thread schedulerThread;

    

    /**
     * Constructor with custom KeyspacesConfig
     * 
     * @param keyspacesConfig The KeyspacesConfig to use
     */
    public KCLScheduler(KeyspacesConfig keyspacesConfig) {
        this.keyspacesConfig = keyspacesConfig;

        ITargetMapper targetMapper = keyspacesConfig.getTargetMapper();

        
        recordProcessorFactory = new KeyspacesRecordProcessorFactory(targetMapper);
    

        streamsClient = KeyspacesStreamsClient.builder()
                .region(Region.of(keyspacesConfig.getRegion()))
                .build();
       
        adapterClient = new AmazonKeyspacesStreamsAdapterClient(
                streamsClient,
                Region.of(keyspacesConfig.getRegion()));

        dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                .region(Region.of(keyspacesConfig.getRegion()))
                .build();

        cloudWatchClient = CloudWatchAsyncClient.builder()
                .region(Region.of(keyspacesConfig.getRegion()))
                .build();
    }


    public void startScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        schedulerThread = new Thread(() -> scheduler.run());
        schedulerThread.start();
    }

    public void shutdownScheduler() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                schedulerThread.join(30000);
            } catch (InterruptedException e) {
                logger.error("Error while shutting down scheduler", e);
            }
        }
    }

    public Scheduler createScheduler(String workerId) {
        
        String streamArn = keyspacesConfig.getStreamArn();

        String leaseTableName = keyspacesConfig.getApplicationName() + "-lease-table";
        
        ConfigsBuilder configsBuilder = createConfigsBuilder(streamArn, workerId, leaseTableName);
        
        
        // Configure retrieval config for polling
        PollingConfig pollingConfig = new PollingConfig(streamArn, adapterClient);

        // Create the Scheduler
        return StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(pollingConfig),
                streamsClient,
                Region.of(keyspacesConfig.getRegion())
        );
    }

    private ConfigsBuilder createConfigsBuilder(String streamArn, String workerId, String leaseTableName) {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                streamArn,
                leaseTableName,
                adapterClient,
                dynamoDbAsyncClient,
                cloudWatchClient,
                workerId,
                recordProcessorFactory);

        configureCoordinator(configsBuilder.coordinatorConfig());
        configureLeaseManagement(configsBuilder.leaseManagementConfig());
        configureProcessor(configsBuilder.processorConfig());
        configureStreamTracker(configsBuilder, streamArn);

        return configsBuilder;
    }

    private void configureCoordinator(CoordinatorConfig config) {
        config.skipShardSyncAtWorkerInitializationIfLeasesExist(keyspacesConfig.isSkipShardSyncAtWorkerInitializationIfLeasesExist())
                .parentShardPollIntervalMillis(keyspacesConfig.getParentShardPollIntervalMillis())
                .shardConsumerDispatchPollIntervalMillis(keyspacesConfig.getShardConsumerDispatchPollIntervalMillis());
    }

    private void configureLeaseManagement(LeaseManagementConfig config) {
        config.shardSyncIntervalMillis(keyspacesConfig.getShardSyncIntervalMillis())
                .leasesRecoveryAuditorInconsistencyConfidenceThreshold(keyspacesConfig.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold())
                .leasesRecoveryAuditorExecutionFrequencyMillis(keyspacesConfig.getLeasesRecoveryAuditorExecutionFrequencyMillis())
                .leaseAssignmentIntervalMillis(keyspacesConfig.getLeaseAssignmentIntervalMillis());
    }

    private void configureProcessor(ProcessorConfig config) {
        config.callProcessRecordsEvenForEmptyRecordList(keyspacesConfig.isCallProcessRecordsEvenForEmptyRecordList());
    }

    private void configureStreamTracker(ConfigsBuilder configsBuilder, String streamArn) {
        StreamTracker streamTracker = StreamsSchedulerFactory.createSingleStreamTracker(
                streamArn,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        );
        configsBuilder.streamTracker(streamTracker);
    }

    public void deleteAllDdbTables(String baseTableName) {
        List<String> tablesToDelete = Arrays.asList(
                baseTableName,
                baseTableName + "-CoordinatorState",
                baseTableName + "-WorkerMetricStats"
        );

        for (String tableName : tablesToDelete) {
            deleteTable(tableName);
        }
    }

    private void deleteTable(String tableName) {
        DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();

        try {
            DeleteTableResponse response = dynamoDbAsyncClient.deleteTable(deleteTableRequest).get();
            logger.debug("Table deletion response: {}", response);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error deleting table: {} - {}", tableName, e.getMessage(), e);
        }
    }

    

    /**
     * Get the CqlSession from the KeyspacesConfig
     * 
     * @return The CqlSession instance
     */
    public CqlSession getCqlSession() {
        return keyspacesConfig.getSession();
    }

    /**
     * Get the KeyspacesConfig instance
     * 
     * @return The KeyspacesConfig instance
     */
    public KeyspacesConfig getKeyspacesConfig() {
        return keyspacesConfig;
    }
}