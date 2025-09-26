package software.amazon.ssa.streams.filter;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.helpers.StreamHelpers;

public class JexlFilterService implements IFilterService {
     /**
     * Evaluates the JEXL filter expression against a record.
     * 
     * @param record The Keyspaces stream record to evaluate
     * @return true if the record passes the filter, false otherwise
     */
    // JEXL filter expression support
    private JexlEngine jexlEngine;

    private static JexlFilterService INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(JexlFilterService.class);

    private JexlFilterService() {
        // Initialize JEXL engine
        this.jexlEngine = new JexlBuilder()
        .namespaces(java.util.Map.of("converters",  JexlFilterFunctions.class))
        .create();
        
    }
    
    public synchronized static IFilterService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new JexlFilterService();
        }
        return INSTANCE;
    }

    @Override
    public boolean evaluateFilter(KeyspacesStreamsClientRecord record, String filterExpressionString) {
        try {
            JexlExpression jexlFilterExpression = jexlEngine.createExpression(filterExpressionString);
            
            // Convert record data to a Map for JEXL evaluation
            Map<String, Object> recordData = extractRecordDataAsMap(record);
            
            // Create JEXL context with the record data
            JexlContext context = new MapContext(recordData);
            
            
            // Evaluate the expression
            Object result = jexlFilterExpression.evaluate(context);
            
            // Convert result to boolean
            if (result instanceof Boolean) {
                return (Boolean) result;
            } else if (result instanceof String) {
                return Boolean.parseBoolean((String) result);
            } else if (result instanceof Number) {
                return ((Number) result).doubleValue() != 0.0;
            } else {
                logger.warn("JEXL expression returned unexpected type: {} for record", result.getClass().getSimpleName());
                return false;
            }
            
        } catch (Exception e) {
            logger.error("Error evaluating JEXL filter expression for record: {}", e.getMessage(), e);
            return false; // Filter out records that cause evaluation errors
        }
    }
    
    /**
     * Extracts record data as a Map for JEXL evaluation.
     * This method creates a hierarchical map structure that includes:
     * - Record metadata (operation, sequenceNumber, etc.)
     * - newImage data (for INSERT/UPDATE operations)
     * - oldImage data (for UPDATE/DELETE operations)
     * 
     * @param record The Keyspaces stream record
     * @return Map containing all record data for JEXL evaluation
     */
    private Map<String, Object> extractRecordDataAsMap(KeyspacesStreamsClientRecord record) {
        Map<String, Object> data = new HashMap<>();
        
        try {
            // Add metadata
            Map<String, Object> metadata = new HashMap<>();
            //metadata.put("sequenceNumber", record.sequenceNumber());
            //metadata.put("keyspace", record.getRecord().keyspace());
           // metadata.put("table", record.getRecord().table());
            // Note: streamArn, shardId, and approximateArrivalTimestamp may not be available
            // on KeyspacesStreamsClientRecord - using null for now
            
            metadata.put("approximateArrivalTimestamp", record.approximateArrivalTimestamp().toEpochMilli());
            
            metadata.put("operation", StreamHelpers.getOperationType(record.getRecord()).toString());
            
            
            data.put("metadata", metadata);
            
            // Add newImage data (for INSERT/UPDATE operations)
            KeyspacesRow newImage = record.getRecord().newImage();
            
            Map<String, Object> newImageData = extractRowDataAsMap(newImage);
            
            data.put("newImage", newImageData);
        
            // Add oldImage data (for UPDATE/DELETE operations)
            KeyspacesRow oldImage = record.getRecord().oldImage();
            
            Map<String, Object> oldImageData = extractRowDataAsMap(oldImage);
            
            data.put("oldImage", oldImageData);
            
        } catch (Exception e) {
            logger.error("Error extracting record data for JEXL evaluation", e);
        }
        
        return data;
    }
    
    /**
     * Extracts data from a KeyspacesRow as a Map.
     * 
     * @param row The Keyspaces row to extract data from
     * @return Map containing the row data
     */
    private Map<String, Object> extractRowDataAsMap(KeyspacesRow row) {
        Map<String, Object> rowData = new HashMap<>();
        
        if (row != null && row.valueCells() != null) {
            for (Map.Entry<String, software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell> entry : row.valueCells().entrySet()) {
                    Object value = StreamHelpers.getValueFromCell(entry.getValue().value());
                    rowData.put(entry.getKey(), value);
            }
        }else{
            return null;
        }
        
        return rowData;
    }
}
