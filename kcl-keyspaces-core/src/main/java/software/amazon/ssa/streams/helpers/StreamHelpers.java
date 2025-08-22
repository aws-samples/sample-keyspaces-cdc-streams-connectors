package software.amazon.ssa.streams.helpers;

import java.util.Map;

import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.awssdk.services.keyspacesstreams.model.OriginType;

import software.amazon.awssdk.services.keyspacesstreams.model.Record;

public class StreamHelpers {
    
    public static StreamProcessorOperationType getOperationType(Record record) throws Exception {
 
        OriginType originType = record.origin();
        
        KeyspacesRow newImage = record.newImage();
        
        KeyspacesRow oldImage = record.oldImage();

        StreamProcessorOperationType operation_type = StreamProcessorOperationType.UNKNOWN;

        if(originType==OriginType.TTL){
            operation_type = StreamProcessorOperationType.TTL;
        }else if(newImage == null){
            if (originType==OriginType.REPLICATION){
                operation_type = StreamProcessorOperationType.REPLICATED_DELETE;
            }else if (originType==OriginType.USER) {
                operation_type = StreamProcessorOperationType.DELETE;
            }else {
                
                throw new Exception("new image is null. Unsupported origin type: " + originType);
            }
        }else if (oldImage == null){
            if (originType==OriginType.REPLICATION){
                operation_type = StreamProcessorOperationType.REPLICATED_INSERT;
            }else if (originType==OriginType.USER) {
                operation_type = StreamProcessorOperationType.INSERT;
            } else {
                    throw new Exception("old image is null. Unsupported origin type: " + originType);
            }
        }else {
            if (originType==OriginType.REPLICATION){
                operation_type = StreamProcessorOperationType.REPLICATED_UPDATE;
            }else if (originType==OriginType.USER) {
                operation_type = StreamProcessorOperationType.UPDATE;
            }else {
                throw new Exception("new image and old image are not null. Unsupported origin type: " + originType);
            }
        }
        return operation_type;
    }
      
    public static Object getValueFromCell(Map.Entry<String, KeyspacesCell> cell) {
        return getValueFromCell(cell.getValue());
    }
    public static Object getValueFromCell(KeyspacesCell cell) {
        KeyspacesCellValue value = cell.value();
        return getValueFromCell(value);
    }
    public static Object getValueFromCell(KeyspacesCellValue value) {
        String cqlType = value.type().name().toLowerCase();
        
        switch (cqlType) {
            case "textt":
            case "varchart":
            case "asciit":
            case "inett":
                return value.textT();
                
            case "intt":
            case "smallintt":
            case "tinyintt":
                return Integer.parseInt(value.intT());
                
            case "bigintt":
            case "countert":
                return Long.parseLong(value.bigintT());
                
            case "floatt":
                return Float.parseFloat(value.floatT());
                
            case "doublet":
                return Double.parseDouble(value.doubleT());
                
            case "booleant":
                return value.boolT();
                
            case "timestampt":
                return value.timestampT();
                
            case "blobt":
                return value.blobT().asByteArray();
                
            default:
                // Return as string for unknown types
                throw new IllegalArgumentException("Unsupported CQL type: " + cqlType);
        }
    }
    public enum StreamProcessorOperationType{
        INSERT("INSERT"),
        UPDATE("UPDATE"),
        DELETE("DELETE"),
        REPLICATED_INSERT("REPLICATED_INSERT"),
        REPLICATED_UPDATE("REPLICATED_UPDATE"),
        REPLICATED_DELETE("REPLICATED_DELETE"),
        TTL("TTL"),
        UNKNOWN("UNKNOWN");

        private final String value;

    
        StreamProcessorOperationType(String value) {
            this.value = value;
        }

        public static StreamProcessorOperationType fromString(String operationType) {
            
            return StreamProcessorOperationType.valueOf(operationType.toUpperCase());
        }
    }
}
