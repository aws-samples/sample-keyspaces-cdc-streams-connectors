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
import java.util.Map;

public class VectorHelper {
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
    
    
    public List<Float> writeRecordsToVectors(String text, Integer dimensions) throws Exception {
    
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
                return asFloat32;
        }
    
}