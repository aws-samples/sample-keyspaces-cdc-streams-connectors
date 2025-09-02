package software.amazon.ssa.streams.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import edu.umd.cs.findbugs.annotations.Nullable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.keyspacesstreams.model.Stream;
import software.amazon.ssa.streams.connector.ITargetMapper;
import software.amazon.awssdk.services.keyspacesstreams.KeyspacesStreamsClient;
import software.amazon.awssdk.services.keyspacesstreams.model.ListStreamsRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.ListStreamsResponse;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Configuration class for Amazon Keyspaces that handles both AWS region configuration
 * and Datastax Java Driver 4 setup for connecting to Keyspaces.
 */
public class KeyspacesConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(KeyspacesConfig.class);
    
    private  String region ;
    private  String streamName ;
    private  String tableName ;
    private  String keyspaceName ;
    private  String serviceEndpoint ;
    private  String streamArn ;
    private  CqlSession session;
    private  KeyspacesStreamsClient keyspacesStreamsClient;
    private  String applicationName;
    private  String configPath;
    private  boolean skipShardSyncAtWorkerInitializationIfLeasesExist;
    private  int parentShardPollIntervalMillis;
    private  int shardConsumerDispatchPollIntervalMillis;
    private  int shardSyncIntervalMillis;
    private  int leasesRecoveryAuditorInconsistencyConfidenceThreshold;
    private  int leasesRecoveryAuditorExecutionFrequencyMillis;
    private  Long leaseAssignmentIntervalMillis;
    private  boolean callProcessRecordsEvenForEmptyRecordList;





    public boolean isSkipShardSyncAtWorkerInitializationIfLeasesExist() {
        return skipShardSyncAtWorkerInitializationIfLeasesExist;
    }
    
    public int getParentShardPollIntervalMillis() {
        return parentShardPollIntervalMillis;
    }
    
    public int getShardConsumerDispatchPollIntervalMillis() {
        return shardConsumerDispatchPollIntervalMillis;
    }
    
    
    public int getShardSyncIntervalMillis() {
        return shardSyncIntervalMillis;
    }
    
    public int getLeasesRecoveryAuditorInconsistencyConfidenceThreshold() {
        return leasesRecoveryAuditorInconsistencyConfidenceThreshold;
    }   
    
    public int getLeasesRecoveryAuditorExecutionFrequencyMillis() {
        return leasesRecoveryAuditorExecutionFrequencyMillis;
    }
    
    public Long getLeaseAssignmentIntervalMillis() {
        return leaseAssignmentIntervalMillis;
    }

    public boolean isCallProcessRecordsEvenForEmptyRecordList() {
        return callProcessRecordsEvenForEmptyRecordList;
    }
    
    
    public KeyspacesConfig(String configPath) {

        this.configPath = configPath;
        initializeConfig();
    }
    
    /**
     * Get the AWS region
     * 
     * @return The configured region
     */
    public String getRegion() {
        return region;
    }
    
    /**
     * Get the keyspace name
     * 
     * @return The configured keyspace name
     */
    public String getKeyspaceName() {
        return keyspaceName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getStreamName() {
        return streamName;
    }
    /**
     * Get the service endpoint for the region
     * 
     * @return The service endpoint URL
     */
    public String getServiceEndpoint() {
        return serviceEndpoint;
    }
    
    /**
     * Get the CqlSession instance
     * 
     * @return The configured CqlSession
     */
    public CqlSession getSession() {
        return session;
    }

    public String getApplicationName() {
        return applicationName;
    }
    
    public synchronized CqlSession getOrCreateCqlSession() {
        if (session == null) {
            session = CqlSession.builder()
            .withConfigLoader(DriverConfigLoader.fromClasspath(configPath))
            .build();
        }
        return session;
    }
    
    public synchronized KeyspacesStreamsClient getOrCreateKeyspacesStreamsClient() {
        if (keyspacesStreamsClient == null) {
            keyspacesStreamsClient = KeyspacesStreamsClient.builder()
            .region(Region.of(region))
            .build();
        }
        return keyspacesStreamsClient;
    }
    /**
     * Gets the latest stream ARN for a given keyspace and table name.
     * 
     * @param keyspaceName The name of the keyspace
     * @param tableName The name of the table
     * @return The latest stream ARN, or null if no stream is found
     */
    public String getStreamArn() {
        try {
            if(streamArn != null && !streamArn.isEmpty()) {
                return streamArn;
            }

            ListStreamsRequest listStreamsRequest = ListStreamsRequest.builder()
                    .keyspaceName(keyspaceName)
                    .tableName(tableName)
                    .build();

            ListStreamsResponse listStreamsResponse = getOrCreateKeyspacesStreamsClient()
               .listStreams(listStreamsRequest);
            
            for (Stream stream : listStreamsResponse.streams()) {
                if(streamName  != null && !streamName.isEmpty()) {
                    if (stream.keyspaceName().equalsIgnoreCase(keyspaceName) && stream.tableName().equalsIgnoreCase(tableName) 
                    && stream.streamLabel().equalsIgnoreCase(streamName)) {
                        return stream.streamArn();
                    }
                }else{
                    if (stream.keyspaceName().equalsIgnoreCase(keyspaceName) && stream.tableName().equalsIgnoreCase(tableName)) {
                        return stream.streamArn();
                    }
                }
            }
           
        } catch (Exception e) {
            logger.error("Error getting stream ARN for keyspace: {}, table: {} - {}", 
                        keyspaceName, tableName, e.getMessage(), e);
            
        }
        return null;
    }
    /**
     * Helper method to get configuration value with environment variable override.
     * Environment variables are named by replacing dots and hyphens with underscores,
     * and removing the "keyspaces-cdc-streams" prefix.
     * 
     * @param conf The configuration object
     * @param configPath The configuration path
     * @param defaultValue The default value if not found
     * @return The configuration value (environment variable takes precedence)
     */
    public static String getConfigValue(Config conf, String configPath, String defaultValue, boolean isRequired) {
        // Convert config path to environment variable name
        String envVarName = convertToEnvVarName(configPath);
        String envValue = System.getenv(envVarName);
        
        if (envValue != null) {
            logger.info("Using environment variable {} for config path {}", envVarName, configPath);
            return envValue;
        }
        if(isRequired && !conf.hasPath(configPath)) {
            throw new IllegalArgumentException("Configuration value is required: " + configPath);
        }
        
        return conf.hasPath(configPath) ? conf.getString(configPath) : defaultValue;
    }
    public static List<String> getConfigValue(Config conf, String configPath, List<String> defaultValue, boolean isRequired) {
        // Convert config path to environment variable name
        String envVarName = convertToEnvVarName(configPath);
        String envValue = System.getenv(envVarName);
        
        if (envValue != null) {
            logger.info("Using environment variable {} for config path {}", envVarName, configPath);
            return Arrays.asList(envValue.split(","));
        }

        if(isRequired && !conf.hasPath(configPath)) {
            throw new IllegalArgumentException("Configuration value is required: " + configPath);
        }
        
        return conf.hasPath(configPath) ? conf.getStringList(configPath) : defaultValue;
    }
    
    /**
     * Helper method to get boolean configuration value with environment variable override.
     */
    public static boolean getConfigValue(Config conf, String configPath, boolean defaultValue, boolean isRequired) {
        String envVarName = convertToEnvVarName(configPath);
        String envValue = System.getenv(envVarName);
        
        if (envValue != null) {
            logger.info("Using environment variable {} for config path {}", envVarName, configPath);
            return Boolean.parseBoolean(envValue);
        }
        
        if(isRequired && !conf.hasPath(configPath)) {
            throw new IllegalArgumentException("Configuration value is required: " + configPath);
        }

        return conf.hasPath(configPath) ? conf.getBoolean(configPath) : defaultValue;
    }
    
    /**
     * Helper method to get integer configuration value with environment variable override.
     */
    public static int getConfigValue(Config conf, String configPath, int defaultValue, boolean isRequired) {
        String envVarName = convertToEnvVarName(configPath);
        String envValue = System.getenv(envVarName);
        
        if (envValue != null) {
            logger.info("Using environment variable {} for config path {}", envVarName, configPath);
            try {
                return Integer.parseInt(envValue);
            } catch (NumberFormatException e) {
                logger.warn("Invalid integer value in environment variable {}: {}", envVarName, envValue);
                return defaultValue;
            }
        }
        if(isRequired && !conf.hasPath(configPath)) {
            throw new IllegalArgumentException("Configuration value is required: " + configPath);
        }
        
        return conf.hasPath(configPath) ? conf.getInt(configPath) : defaultValue;
    }
    
    /**
     * Helper method to get long configuration value with environment variable override.
     */
    public static long getConfigValue(Config conf, String configPath, long defaultValue, boolean isRequired) {
        String envVarName = convertToEnvVarName(configPath);
        String envValue = System.getenv(envVarName);
        
        if (envValue != null) {
            logger.info("Using environment variable {} for config path {}", envVarName, configPath);
            try {
                return Long.parseLong(envValue);
            } catch (NumberFormatException e) {
                logger.warn("Invalid long value in environment variable {}: {}", envVarName, envValue);
                return defaultValue;
            }
        }
        if(isRequired && !conf.hasPath(configPath)) {
            throw new IllegalArgumentException("Configuration value is required: " + configPath);
        }
        
        return conf.hasPath(configPath) ? conf.getLong(configPath) : defaultValue;
    }
    
    /**
     * Convert configuration path to environment variable name.
     * Removes "keyspaces-cdc-streams" prefix and replaces dots and hyphens with underscores.
     * 
     * @param configPath The configuration path
     * @return The environment variable name
     */
    public static String convertToEnvVarName(String configPath) {
        // Remove "keyspaces-cdc-streams" prefix
        String withoutPrefix = configPath.replaceFirst("^keyspaces-cdc-streams\\.", "");
        
        // Replace dots and hyphens with underscores
        String envVarName = withoutPrefix.replaceAll("[\\.-]", "_").toLowerCase();
        
        // Convert to uppercase
        return envVarName.toUpperCase();
    }
    
    /**
     * Create and configure the CqlSession for Amazon Keyspaces
     * 
     * @return The configured CqlSession
     */
    private void initializeConfig() {
       
        Config conf = ConfigFactory.load(configPath);

        keyspaceName = getConfigValue(conf, "keyspaces-cdc-streams.stream.keyspace-name", "", false);
        tableName = getConfigValue(conf, "keyspaces-cdc-streams.stream.table-name", "", false);
        streamName = getConfigValue(conf, "keyspaces-cdc-streams.stream.stream-name", "", false);
        region = getConfigValue(conf, "keyspaces-cdc-streams.stream.region", "", true);
        streamArn = getConfigValue(conf, "keyspaces-cdc-streams.stream.stream-arn", "", false);
        applicationName = getConfigValue(conf, "keyspaces-cdc-streams.stream.application-name", "my-stream-app", true);
        
        skipShardSyncAtWorkerInitializationIfLeasesExist = getConfigValue(conf, "keyspaces-cdc-streams.coordinator.skip-shard-sync-at-worker-initialization-if-leases-exist", true, false);
        parentShardPollIntervalMillis = getConfigValue(conf, "keyspaces-cdc-streams.coordinator.parent-shard-poll-interval-millis", 1000, false);
        shardConsumerDispatchPollIntervalMillis = getConfigValue(conf, "keyspaces-cdc-streams.coordinator.shard-consumer-dispatch-poll-interval-millis", 500, false);

       
        shardSyncIntervalMillis = getConfigValue(conf, "keyspaces-cdc-streams.lease-management.shard-sync-interval-millis", 60000, false);
        leasesRecoveryAuditorInconsistencyConfidenceThreshold = getConfigValue(conf, "keyspaces-cdc-streams.lease-management.leases-recovery-auditor-inconsistency-confidence-threshold", 3, false);
        leasesRecoveryAuditorExecutionFrequencyMillis = getConfigValue(conf, "keyspaces-cdc-streams.lease-management.leases-recovery-auditor-execution-frequency-millis", 5000, false);
        leaseAssignmentIntervalMillis = getConfigValue(conf, "keyspaces-cdc-streams.lease-management.lease-assignment-interval-millis", 1000L, false);

        callProcessRecordsEvenForEmptyRecordList = getConfigValue(conf, "keyspaces-cdc-streams.processor.call-process-records-even-for-empty-record-list", true, false);

    }

    public ITargetMapper getTargetMapper() {

        Config conf = ConfigFactory.load(configPath);

        return buildFromConfig(conf,
            null,
            "keyspaces-cdc-streams.connector.target-mapper",
            ITargetMapper.class,
            "software.amazon.ssa.streams.connector")
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Missing default targer mapper, check your configuration (%s)",
                        "software.amazon.ssa.streams.connector.target.DefaultKeyspacesTargetMapper")));
  }
    
    
    
    /**
     * Close the CqlSession
     */
    public void close() {
        if (session != null && !session.isClosed()) {
            session.close();
        }
    }
    
   

@Nullable
  public static Class<?> loadClass(@Nullable ClassLoader classLoader, @NonNull String className) {
    try {
      Class<?> clazz;
      if (classLoader == null) {
        logger.trace("Attempting to load {} with driver's class loader", className);
        clazz = Class.forName(className);
      } else {
        logger.trace("Attempting to load {} with {}", className, classLoader);
        clazz = Class.forName(className, true, classLoader);
      }
      logger.trace("Successfully loaded {}", className);
      return clazz;
    } catch (LinkageError | Exception e) {
      // Note: only ClassNotFoundException, LinkageError and SecurityException
      // are declared to be thrown; however some class loaders (Apache Felix)
      // may throw other checked exceptions, which cannot be caught directly
      // because that would cause a compilation failure.
      logger.debug(
          String.format("Could not load %s with loader %s: %s", className, classLoader, e), e);
      if (classLoader == null) {
        return null;
      } else {
        // If the user-supplied class loader is unable to locate the class, try with the driver's
        // default class loader. This is useful in OSGi deployments where the user-supplied loader
        // may be able to load some classes but not all of them. Besides, the driver bundle, in
        // OSGi, has a "Dynamic-Import:*" directive that makes its class loader capable of locating
        // a great number of classes.
        return loadClass(null, className);
      }
    }
  }
  public static <ComponentT> Optional<ComponentT> buildFromConfig(
      Config config,
      String connectorProfile,
      String classNameOption,
      Class<ComponentT> expectedSuperType,
      String... defaultPackages) {

   
    logger.debug("Creating a {} from config option {}", expectedSuperType.getSimpleName(), classNameOption);

    if (!config.hasPath(classNameOption)) {
      logger.debug("Option is not defined, skipping");
      return Optional.empty();
    }

    String className = config.getString(classNameOption);
    return Optional.of(
        resolveClass(
            config, connectorProfile, expectedSuperType, classNameOption, className, defaultPackages));
  }
  @NonNull
  private static <ComponentT> ComponentT resolveClass(
      Config config,
      String connectorProfile,
      Class<ComponentT> expectedSuperType,
      String configPath,
      String className,
      String[] defaultPackages) {
    Class<?> clazz = null;
    if (className.contains(".")) {
      logger.debug("Building from fully-qualified name {}", className);
      clazz = loadClass(config.getClass().getClassLoader(), className);
    } else {
        logger.debug("Building from unqualified name {}", className);
      for (String defaultPackage : defaultPackages) {
        String qualifiedClassName = defaultPackage + "." + className;
        logger.debug("Trying with default package {}", qualifiedClassName);
        clazz = loadClass(config.getClass().getClassLoader(), qualifiedClassName);
        if (clazz != null) {
          break;
        }
      }
    }
    if (clazz == null) {
      throw new IllegalArgumentException(
          String.format("Can't find class %s (specified by %s)", className, configPath));
    }
    Preconditions.checkArgument(
        expectedSuperType.isAssignableFrom(clazz),
        "Expected class %s (specified by %s) to be a subtype of %s",
        className,
        configPath,
        expectedSuperType.getName());

    Constructor<? extends ComponentT> constructor;
    Class<?>[] argumentTypes =
        (connectorProfile == null)
            ? new Class<?>[] {Config.class}
            : new Class<?>[] {Config.class, String.class};
    try {
      constructor = clazz.asSubclass(expectedSuperType).getConstructor( argumentTypes);//.getConstructor(argumentTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Expected class %s (specified by %s) "
                  + "to have an accessible constructor with arguments (%s)",
              className, configPath));//, Joiner.on(',').join(argumentTypes)));
    }
    try {
      @SuppressWarnings("JavaReflectionInvocation")
      ComponentT instance = constructor.newInstance(config);
      return instance;
    } catch (Exception e) {
      // ITE just wraps an exception thrown by the constructor, get rid of it:
      Throwable cause = (e instanceof InvocationTargetException) ? e.getCause() : e;
      throw new IllegalArgumentException(
          String.format(
              "Error instantiating class %s (specified by %s): %s",
              className, configPath, cause.getMessage()),
          cause);
    }
  }

} 