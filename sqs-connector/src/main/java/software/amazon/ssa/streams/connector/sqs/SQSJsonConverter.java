package software.amazon.ssa.streams.connector.sqs;


import com.typesafe.config.Config;

import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.ssa.streams.converters.AbstractJSONConverter;

public class SQSJsonConverter extends AbstractJSONConverter<SendMessageBatchRequestEntry>{
    
    private int delaySeconds;

    public SQSJsonConverter(Config config,int delaySeconds){
        super( config);
        this.delaySeconds = delaySeconds;
    }

    @Override
    public SendMessageBatchRequestEntry convertRecordToMessage(String id, byte[] body) throws Exception {
        return SendMessageBatchRequestEntry.builder()
            .id(id)
            .delaySeconds(delaySeconds)
            .messageBody(new String(body))
            .build();
    }
}
