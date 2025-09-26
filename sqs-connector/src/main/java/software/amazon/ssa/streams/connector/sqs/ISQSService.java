package software.amazon.ssa.streams.connector.sqs;


import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

public interface ISQSService {
    public SqsClient getOrCreateSqsClient(String region);

    public SendMessageBatchResponse sendBatchRequest(SendMessageBatchRequest batchRequest);
}
