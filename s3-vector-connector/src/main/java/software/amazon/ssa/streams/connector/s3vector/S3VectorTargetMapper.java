package software.amazon.ssa.streams.connector.s3vector;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import java.math.BigDecimal;
import java.util.ArrayList;

import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.core.document.Document.MapBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.PutInputVector;
import software.amazon.awssdk.services.s3vectors.model.PutVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.VectorData;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.connector.AbstractTargetMapper;
import software.amazon.ssa.streams.helpers.StreamHelpers;
import software.amazon.ssa.streams.helpers.VectorHelper;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.Type;
import software.amazon.awssdk.services.s3vectors.model.PutInputVector.Builder;

/**
 * S3 Vector Target Mapper for Amazon Keyspaces CDC Streams
 * 
 * This connector writes Keyspaces CDC records to Amazon S3 Vector Store with vector embeddings.
 * It uses Amazon Bedrock to generate embeddings from text fields and stores them in S3 Vector Store
 * for similarity search and vector operations.
 * 
 * Configuration:
 * - bucket-id: S3 Vector bucket name (required)
 * - region: AWS region (default: us-east-1)
 * - max-retries: Maximum retry attempts for S3 Vector operations (default: 3)
 * - embedding-model: Bedrock model for generating embeddings (default: amazon.titan-embed-text-v2:0)
 * - index-name: S3 Vector index name (required)
 * - embedding-field: Field name to generate embeddings from (required)
 * - key-field: Field name to use as vector key (required)
 * - metadata-fields: List of fields to include as metadata (optional)
 * - dimensions: Vector dimensions (default: 256)
 */
public class S3VectorTargetMapper extends AbstractTargetMapper {

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
   
    public S3VectorTargetMapper(Config config) {
        super(config);
        this.bucketName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.bucket-id", "", true);
        this.regionName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.region", "us-east-1", true);
        this.maxRetries = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.max-retries", 3, false);
        this.embeddingModel = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.embedding-model", "amazon.titan-embed-text-v2:0", false);
        this.indexName = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.index-name", "", false);
        this.embeddingField = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.embedding-field", "", false);
        this.keyField = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.key-field", "", false);
        this.metadataFields = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.metadata-fields", new ArrayList<String>(), false);
        this.dimensions = KeyspacesConfig.getConfigValue(config, "keyspaces-cdc-streams.stream.connector.dimensions", 256, false);
       
        this.vectorHelper = new VectorHelper(embeddingModel, regionName);
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
                
                String fieldName = entry.getKey();
                Type cellType = entry.getValue().value().type();
                Class<?> javaType = StreamHelpers.mapCqlTypeToJavaType(cellType);

                logger.info("fieldName: {}, cellType: {}, javaType: {}", fieldName, cellType, javaType);
                if(fieldName.equals(embeddingField)){
                    if(javaType == String.class){
                        String text = entry.getValue().value().textT();

                        if(text == null || text.isEmpty()){
                            logger.warn("record with empty embedding field: {}", record.getRecord().newImage().toString());
                            String key = record.getRecord().newImage().valueCells().get(keyField).value().textT();
                               
                            if(key != null && !key.isEmpty()){
                                text = key;
                               
                                logger.warn("rusing key field: {}", key);

                            } else {
                                logger.warn("record with empty embedding field: {} and no key field", record.getRecord().newImage().toString());
                           
                                throw new IllegalArgumentException("No descrption or key field found for recond " + record.getRecord().newImage().toString());
                            }
                        }
                        

                        List<Float> asFloat32 = vectorHelper.writeRecordsToVectors(text, dimensions, maxRetries);
                        putInputVectorBuilder.data(VectorData.builder().float32(asFloat32).build());
                    } else {
                        throw new IllegalArgumentException("Unsupported CQL type for vector index embedding: " + cellType);
                    }
                } else if(fieldName.equals(keyField)){
                    if(javaType == String.class){
                        String text = entry.getValue().value().textT();
                        putInputVectorBuilder.key(text);
                    } else {
                        throw new IllegalArgumentException("Unsupported CQL type for vector index key: " + cellType);
                    }
                }
                if (metadataFields.contains(fieldName)){
                    if(javaType == String.class){
                        String text = entry.getValue().value().textT();
                        if(text != null){
                            metaDataFieldsDocument.putString(fieldName, text);
                        }
                    } else if(javaType == Integer.class){
                        Integer number = (Integer)StreamHelpers.getValueFromCell(entry);
                        metaDataFieldsDocument.putNumber(fieldName, number);
                    } else if(javaType == Long.class){
                        Long number = (Long)StreamHelpers.getValueFromCell(entry);
                        metaDataFieldsDocument.putNumber(fieldName, number);
                    } else if(javaType == Float.class){
                        Float number = (Float)StreamHelpers.getValueFromCell(entry);
                        metaDataFieldsDocument.putNumber(fieldName, number);
                    } else if(javaType == Double.class){
                        Double number = (Double)StreamHelpers.getValueFromCell(entry);
                        metaDataFieldsDocument.putNumber(fieldName, number);
                    } else if(javaType == Boolean.class){
                        Boolean booleanValue = (Boolean)StreamHelpers.getValueFromCell(entry);
                        metaDataFieldsDocument.putBoolean(fieldName, booleanValue);
                    } else if(javaType == BigDecimal.class){
                        BigDecimal number = (BigDecimal)StreamHelpers.getValueFromCell(entry);
                        metaDataFieldsDocument.putNumber(fieldName, number);
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported CQL type for vector index metadata: " + cellType);
                    }
                }
            }
            putInputVectorBuilder.metadata(metaDataFieldsDocument.build());
            toPut.add(putInputVectorBuilder.build());
        }
           
           
        PutVectorsRequest putReq = PutVectorsRequest.builder()
            .vectorBucketName(bucketName)
            .indexName(indexName)
            .vectors(toPut)
            .build();
        
        S3VectorsClient s3VectorsClient = getOrCreateS3VectorsClient();
        
        s3VectorsClient.putVectors(putReq);
        
        logger.debug("Successfully wrote {} records to S3Vector index: {} bucket: {}", records.size(), indexName, bucketName);
        
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
