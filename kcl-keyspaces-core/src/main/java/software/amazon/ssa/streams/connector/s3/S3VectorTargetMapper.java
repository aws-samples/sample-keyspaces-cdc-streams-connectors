package software.amazon.ssa.streams.connector.s3vector;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import java.util.ArrayList;

import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.core.document.Document.MapBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.PutInputVector;
import software.amazon.awssdk.services.s3vectors.model.PutVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.PutVectorsResponse;
import software.amazon.awssdk.services.s3vectors.model.VectorData;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.ITargetMapper;
import software.amazon.ssa.streams.helpers.StreamHelpers;
import software.amazon.ssa.streams.helpers.VectorHelper;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.Type;
import software.amazon.awssdk.services.s3vectors.model.PutInputVector.Builder;

public class S3VectorTargetMapper implements ITargetMapper {

    private static final Logger logger = LoggerFactory.getLogger(S3VectorTargetMapper.class);
    
    private String bucketName;
    private String regionName;
    private int maxRetries;
    private String embeddingModel;
    private String indexName;
    private String embeddingField;
    private String keyField;
    private List<String> metadataFields;
    private VectorHelper vectorHelper;
    private int dimensions;
    private S3VectorsClient s3VectorsClient;
    
   // Schema definition for Keyspaces CDC records with valueCells structure
   
    public S3VectorTargetMapper(Config config) {

        this.bucketName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.bucket-id", "", true);
        this.regionName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.region", "us-east-1", true);
        this.maxRetries = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.max-retries", 3, false);
        this.embeddingModel = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.embedding-model", "amazon.titan-embed-text-v2:0", false);
        this.indexName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.index-name", "", false);
        this.embeddingField = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.embedding-field", "", false);
        this.keyField = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.key-field", "", false);
        this.metadataFields = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.metadata-fields", new ArrayList<String>(), false);
        this.dimensions = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.connector.dimensions", 256, false);
       
        this.vectorHelper = new VectorHelper(embeddingModel, regionName);
    }

    @Override
    public void initialize(KeyspacesConfig keyspacesConfig) {
        logger.info("Initializing S3VectorTargetMapper with bucket: {} and index: {}", bucketName, indexName);
    }

    @Override
    public void handleRecords(List<KeyspacesStreamsClientRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            logger.debug("No records to process");
            return;
        }

        List<PutInputVector> toPut = new ArrayList<>();
               
            for (KeyspacesStreamsClientRecord record : records) {

                Builder putInputVectorBuilder = PutInputVector.builder();

                MapBuilder metaDataFieldsDocument = Document.mapBuilder();

                for (Map.Entry<String, KeyspacesCell> entry : record.getRecord().newImage().valueCells().entrySet()) {
                        
                        Type cellType = entry.getValue().value().type();
                            
                        Class<?> javaType = StreamHelpers.mapCqlTypeToJavaType(cellType);

                        if(entry.getKey().equals(embeddingField)){
                           
                            if(javaType == String.class){
                                    String text = entry.getValue().value().textT();

                                    List<Float> asFloat32 = vectorHelper.writeRecordsToVectors(text, dimensions);
                                    
                                    putInputVectorBuilder.data(VectorData.builder().float32(asFloat32).build());
                            }else {
                                throw new IllegalArgumentException("Unsupported CQL type for vector index embedding: " + cellType);
                            }
                               
                        }else if(entry.getKey().equals(keyField)){
                            
                            if(javaType == String.class){
                                String text = entry.getValue().value().textT();
                                
                                putInputVectorBuilder.key(text);

                            }else{
                                throw new IllegalArgumentException("Unsupported CQL type for vector index key: " + cellType);
                            }
                        }else if (metadataFields.contains(entry.getKey())){
                            
                            if(javaType == String.class){
                                String text = entry.getValue().value().textT();
                                
                                metaDataFieldsDocument
                                        .putString(entry.getKey(), text);
                            }else if(javaType == Integer.class){
                                Integer number = (Integer)StreamHelpers.getValueFromCell(entry);
                                
                                metaDataFieldsDocument.putNumber(bucketName, number);

                            }else if(javaType == Long.class){
                                Long number = (Long)StreamHelpers.getValueFromCell(entry);
                                
                                metaDataFieldsDocument.putNumber(bucketName, number);
                                
                            }else if(javaType == Float.class){
                                Float number =  (Float)StreamHelpers.getValueFromCell(entry);
                                
                                metaDataFieldsDocument.putNumber(bucketName, number);
                            }else if(javaType == Double.class){
                                Double number =  (Double)StreamHelpers.getValueFromCell(entry);
                                
                                metaDataFieldsDocument.putNumber(bucketName, number);
                            }else if(javaType == Boolean.class){
                                Boolean booleanValue = (Boolean)StreamHelpers.getValueFromCell(entry);
                                
                                metaDataFieldsDocument.putBoolean(bucketName, booleanValue);
                            }else{
                                throw new IllegalArgumentException("Unsupported CQL type for vector index metadata: " + cellType);
                            }
                        }
                }
                putInputVectorBuilder.metadata(metaDataFieldsDocument.build());

                toPut.add(putInputVectorBuilder.build());
                
            }
            // 5) Put vectors into the index
           
        boolean success = false;
        
        for (int attempt = 0; attempt < maxRetries && !success; attempt++) {
            try {
                PutVectorsRequest putReq = PutVectorsRequest.builder()
                .vectorBucketName(bucketName)
                .indexName(indexName)
                .vectors(toPut)
                .build();
                
                S3VectorsClient s3VectorsClient = getOrCreateS3VectorsClient();

                PutVectorsResponse resp = s3VectorsClient.putVectors(putReq);
                
                success = true;
                
                logger.debug("Successfully wrote {} records to S3Vector index: {} bucket: {}", records.size(), indexName, bucketName);
            } catch (Exception s3Error) {
                logger.warn("S3Vector write attempt {} failed: {}", attempt, s3Error.getMessage());
                if (attempt < maxRetries-1) {
                    Thread.sleep(10 * attempt); // Exponential backoff
                } else {
                    throw s3Error;
                }
            }
        }
     }
     private synchronized S3VectorsClient getOrCreateS3VectorsClient(){
        if(s3VectorsClient == null){
            s3VectorsClient = S3VectorsClient.builder()
                .region(Region.of(regionName))
                .build();
        }
        return s3VectorsClient;
    }
}
