package software.amazon.ssa.streams.helpers;

// Maven deps you’ll need:
//


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.core.document.Document.MapBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.PutInputVector;
import software.amazon.awssdk.services.s3vectors.model.PutVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.PutVectorsResponse;
import software.amazon.awssdk.services.s3vectors.model.VectorData;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.Type;
import software.amazon.awssdk.services.s3vectors.model.PutInputVector.Builder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorHelper {
    private static final Logger logger = LoggerFactory.getLogger(VectorHelper.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    // Retry configuration
    private static final long BASE_DELAY_MS = 1000; // 1 second base delay
    private static final long MAX_DELAY_MS = 10000; // 10 seconds max delay
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final double JITTER_FACTOR = 0.1; // 10% jitter
    private static final Random RANDOM = new Random();

    private String modelId;
    private String region;
    private BedrockRuntimeClient bedrockRuntimeClient;

    public VectorHelper(String modelId, String region){
        this.modelId = modelId;
        this.region = region;

        if(region == null || region.isEmpty()){
            throw new IllegalArgumentException("Region is required");
        }
        
    }
    private synchronized BedrockRuntimeClient getOrCreateBedrockClient(){
        if(bedrockRuntimeClient == null){
                bedrockRuntimeClient = BedrockRuntimeClient.builder()
                .region(Region.of(region))
                .build();
        }
        return bedrockRuntimeClient;
    }
    
    /**
     * Calculate exponential backoff delay with jitter
     * @param attemptNumber The current attempt number (0-based)
     * @return Delay in milliseconds
     */
    private long calculateBackoffDelay(int attemptNumber) {
        // Calculate exponential backoff: baseDelay * (backoffMultiplier ^ attemptNumber)
        long delay = (long) (BASE_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, attemptNumber));
        
        // Cap at maximum delay
        delay = Math.min(delay, MAX_DELAY_MS);
        
        // Add jitter to prevent thundering herd
        double jitter = delay * JITTER_FACTOR * (RANDOM.nextDouble() - 0.5);
        delay = (long) (delay + jitter);
        
        // Ensure delay is at least 0
        return Math.max(0, delay);
    }
    
    /**
     * Check if an exception should be retried
     * @param exception The exception to check
     * @return true if the exception should be retried
     */
    private boolean shouldRetry(Exception exception) {
        // Retry on common transient errors
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        
        // Retry on throttling, service unavailable, or timeout errors
        return message.contains("ThrottlingException") ||
               message.contains("ServiceUnavailableException") ||
               message.contains("InternalServerError") ||
               message.contains("RequestTimeoutException") ||
               message.contains("TooManyRequestsException") ||
               message.contains("502") || // Bad Gateway
               message.contains("503") || // Service Unavailable
               message.contains("504");   // Gateway Timeout
    }
    
    
    public List<Float> writeRecordsToVectors(String text, Integer dimensions, Integer maxRetries) throws Exception {
        String requestJson = MAPPER.createObjectNode()
                .put("inputText", text)
                .put("dimensions", dimensions)
                .toString();

        InvokeModelRequest req = InvokeModelRequest.builder()
                .modelId(modelId)
                .contentType("application/json")
                .accept("application/json")
                .body(SdkBytes.fromString(requestJson, StandardCharsets.UTF_8))
                .build();

        BedrockRuntimeClient bedrockRuntimeClient = getOrCreateBedrockClient();
        
        Exception lastException = null;
        
        // Retry loop with exponential backoff
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                logger.debug("Invoking Bedrock model {} (attempt {}/{})", modelId, attempt + 1, maxRetries + 1);
                
                InvokeModelResponse resp = bedrockRuntimeClient.invokeModel(req);
                
                // Response body is JSON like: { "embedding": [ ... numbers ... ] }
                JsonNode root = MAPPER.readTree(resp.body().asUtf8String());
                JsonNode emb = root.get("embedding");
                if (emb == null || !emb.isArray()) {
                    throw new IllegalStateException("Missing 'embedding' field in model response");
                }

                List<Float> asFloat32 = new ArrayList<>(emb.size());
                for (JsonNode n : emb) {
                    // Titan returns numbers; convert to Java float (float32) as required by S3 Vectors.
                    asFloat32.add((float) n.asDouble());
                }
                
                if (attempt > 0) {
                    logger.info("Successfully invoked Bedrock model after {} retries", attempt);
                }
                
                return asFloat32;
                
            } catch (Exception e) {
                lastException = e;
                logger.warn("Failed to invoke Bedrock model on attempt {}: {}", attempt + 1, e.getMessage());
                
                // Check if we should retry this exception
                if (attempt < maxRetries && shouldRetry(e)) {
                    long delay = calculateBackoffDelay(attempt);
                    logger.info("Retrying in {}ms (attempt {}/{})", delay, attempt + 2, maxRetries + 1);
                    
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry delay", ie);
                    }
                } else {
                    // Don't retry if we've exhausted attempts or exception is not retryable
                    break;
                }
            }
        }
        
        // If we get here, all retries failed
        logger.error("Failed to invoke Bedrock model after {} attempts", maxRetries + 1);
        throw new RuntimeException("Failed to invoke Bedrock model after " + (maxRetries + 1) + " attempts", lastException);
    }
    
    
    
}