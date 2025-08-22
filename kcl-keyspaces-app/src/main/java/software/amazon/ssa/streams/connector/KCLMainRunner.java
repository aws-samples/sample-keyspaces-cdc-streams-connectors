package software.amazon.ssa.streams.connector;

import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.helpers.AWSHelpers;
import software.amazon.ssa.streams.scheduler.KCLScheduler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KCLMainRunner {

    private static final Logger logger = LoggerFactory.getLogger(KCLMainRunner.class);
    public static void main(String[] args) {

        Map<String, String> env = System.getenv();
        for (String envName : env.keySet()) {
            System.out.format("%s=%s%n", envName, env.get(envName));
        }

        KeyspacesConfig keyspacesConfig = new KeyspacesConfig("application.conf");
        
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