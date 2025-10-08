package software.amazon.ssa.streams.converters;

import java.util.Collections;
import java.util.Map;

import com.typesafe.config.Config;

public class DefaultAvroConverter extends AbstractAvroConverter<Map<String, byte[]>> {

    public DefaultAvroConverter(Config config){
        super(config);
    }

    @Override
    public Map<String, byte[]> convertRecordToMessage(String id, byte[] body) throws Exception {
        return Collections.singletonMap(id, body);
    }

}