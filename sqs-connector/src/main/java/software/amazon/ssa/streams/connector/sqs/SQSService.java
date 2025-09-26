package software.amazon.ssa.streams.connector.sqs;
import java.util.HashMap;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

public class SQSService implements ISQSService {

    private static HashMap<String, SQSService> I;

    private volatile SqsClient sqsClient;

    private String region;

    private SQSService(String region){
        this.region = region;
    }

    public synchronized static SQSService getInstance(String region) {
        
        if (I == null) {
            I = new HashMap<>();
        }

        if (!I.containsKey(region)) {
            I.put(region, new SQSService(region));
        }
        
        return I.get(region);
    }

    @SuppressWarnings("EI_EXPOSE_REP")
    public synchronized SqsClient getOrCreateSqsClient(String region) {
        if (sqsClient == null) {
            sqsClient = SqsClient.builder().region(Region.of(region)).build();
        }
        // Return a reference to the client - SqsClient is immutable and thread-safe
        return sqsClient;
    }
    public SendMessageBatchResponse sendBatchRequest(SendMessageBatchRequest batchRequest){
        SqsClient client = getOrCreateSqsClient(region);
        return client.sendMessageBatch(batchRequest);
    }

}
