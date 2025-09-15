package software.amazon.ssa.streams.connector;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.helpers.AWSHelpers;
import software.amazon.ssa.streams.scheduler.KCLScheduler;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KCLMainRunner {

    private static final Logger logger = LoggerFactory.getLogger(KCLMainRunner.class);
    public static void main(String[] args) {

        String applicationConfLocation = "streams-application.conf";

    
        Map<String, String> env = System.getenv();
        for (String envName : env.keySet()) {
            System.out.format("%s=%s%n", envName, env.get(envName));
        }
        
        if(env.get("APPLICATION_CONF_LOCATION") != null){
            applicationConfLocation = env.get("APPLICATION_CONF_LOCATION");
        }

        
        if(Files.exists(Paths.get(applicationConfLocation))){
            logger.info("File exists in path: {}", applicationConfLocation);
        } else {

            logger.info("File does not exist in path: {}", applicationConfLocation);

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
       
            URL resource = classLoader.getResource(applicationConfLocation);
            
            if (resource != null) {

                applicationConfLocation = resource.getPath();

                logger.info("File exists in classpath: {}", applicationConfLocation);
                
            } else {
                logger.info("File does not exist in classpath: {}", applicationConfLocation);
               
            }
        }

        // Check if credentials file exists in /home/appuser/.aws/credentials
        if(Files.exists(Paths.get("/home/appuser/.aws/credentials"))){
            logger.info("Credentials file exists in /appuser/.aws/credentials");
        } else {
            logger.info("Credentials file does not exist in /home/appuser/.aws/credentials");
        }

        DefaultCredentialsProvider defaultCredentialsProvider = DefaultCredentialsProvider
                                                                    .builder().build();

        AwsCredentials credentials = defaultCredentialsProvider.resolveCredentials();
        if(credentials == null){
            logger.info("Credentials not found");
            throw new RuntimeException("Credentials file does not exist. SET AWS_CREDENTIALS_LOCATION environment variable to the correct path. Current setting : " + "/app/.aws/credentials");
        }else{
            String accessKey = credentials.accessKeyId();
            logger.info("Credentials found, using Access key: {}", accessKey);
        }

        KeyspacesConfig keyspacesConfig = new KeyspacesConfig(applicationConfLocation);
        
        KCLScheduler kclTestBase = new KCLScheduler(keyspacesConfig);

        String workerId = AWSHelpers.createWorkerIdFromSTS();

        Scheduler scheduler = kclTestBase.createScheduler(workerId);

        logger.info("created scheduler for Worker - {}", workerId);


        AtomicReference schedulerRef = new AtomicReference<>(scheduler);
        
        logger.info("starting scheduler for Worker - {}", workerId);

        CompletableFuture<Void> run = CompletableFuture.runAsync(scheduler::run);
        
        // Graceful on SIGTERM/SIGINT: ask KCL to quiesce and checkpoint
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kclTestBase.shutdownScheduler();
            Scheduler s = (Scheduler) schedulerRef.get();
            if (s != null) {
                s.startGracefulShutdown().join(); // triggers shutdownRequested() in processors
            }
        }));

        logger.info("starting health check server for Worker - {}", workerId);

        try {
            HealthServer.start();
        } catch (Exception e) {
            logger.error("Error starting health check server", e);
        }
        //Block main so PID 1 stays alive
        run.join();
    
        logger.info("Worker - {} completed", workerId);
        //kclTestBase.deleteAllDdbTables(leaseTableName);
    }
}