package software.amazon.ssa.streams.connector.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.ssa.streams.converters.AbstractJSONConverter;

import java.util.Collections;
import java.util.Map;

import com.typesafe.config.Config;



public class S3JsonConverter extends AbstractJSONConverter<Map<String, RequestBody>>{

    public S3JsonConverter(Config config){
        super(config);
    }

    @Override
    public Map<String, RequestBody> convertRecordToMessage(String id, byte[] body) throws Exception {
        
        return Collections.singletonMap(id, RequestBody.fromBytes(body));
    }
    
}
