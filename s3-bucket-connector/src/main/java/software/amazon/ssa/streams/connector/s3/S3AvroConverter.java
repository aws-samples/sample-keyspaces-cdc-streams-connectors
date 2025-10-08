package software.amazon.ssa.streams.connector.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.ssa.streams.converters.AbstractAvroConverter;

import java.util.Collections;
import java.util.Map;

import com.typesafe.config.Config;



public class S3AvroConverter extends AbstractAvroConverter<Map<String, RequestBody>>{

 
    public S3AvroConverter(Config config){
        super(config);
    }

    @Override
    public Map<String, RequestBody> convertRecordToMessage(String id, byte[] body) throws Exception {
        
        return Collections.singletonMap(id, RequestBody.fromBytes(body));
    }
    
}
