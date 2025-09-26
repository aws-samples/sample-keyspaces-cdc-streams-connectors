package software.amazon.ssa.streams.connector;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.ssa.streams.config.KeyspacesConfig;
import software.amazon.ssa.streams.filter.IFilterService;
import software.amazon.ssa.streams.filter.JexlFilterService;
import software.amazon.ssa.streams.helpers.StreamHelpers;

public abstract class AbstractTargetMapper implements ITargetMapper {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTargetMapper.class);

    protected Config  config;
   
    private String filterExpressionString;

    private IFilterService filterService;
    
    public AbstractTargetMapper(Config config) {
        this.config = config;
        
        this.filterExpressionString = KeyspacesConfig.getConfigValue(
            config, 
            "keyspaces-cdc-streams.connector.jexl-filter-expression", 
            (String) null, 
            false
        );
        

    }

    @Override
    public void initialize() {

        if(filterExpressionString != null && !filterExpressionString.trim().isEmpty()) {
            this.filterService = JexlFilterService.getInstance();
        }
        //place holder for parent class implementation
        //constructor called during configuration loading 
        //initialize is called post class loading, pre handling of records
    }
    @Override
    public List<KeyspacesStreamsClientRecord> filterRecords(List<KeyspacesStreamsClientRecord> records) {
        // If no JEXL filter expression is configured, return all records
        if (records == null || records.isEmpty()) {
            return records;
        }

        if (filterExpressionString == null || filterExpressionString.trim().isEmpty()) {
            return records;
        }

        logger.debug("Applying filter expression to {} records: {}", records.size(), filterExpressionString);
        
        if(filterService == null) {
            filterService = JexlFilterService.getInstance();
        }
        // Filter records using JEXL expression
        List<KeyspacesStreamsClientRecord> filteredRecords = records.stream()
            .filter(this::evaluateFilter)
            .collect(Collectors.toList());


        if(filteredRecords.size() < records.size()) {
            for(KeyspacesStreamsClientRecord record : records) {
                if(!filteredRecords.contains(record)) {
                    logger.warn("Record not passed filter: {}", record.getRecord().newImage().toString());
                }
            }

            logger.info("filter applied: {} records passed filter out of {} total records", 
                   filteredRecords.size(), records.size());
        }
        
        
                 
        return filteredRecords;
    }
    private boolean evaluateFilter(KeyspacesStreamsClientRecord record) {
        return filterService.evaluateFilter(record, filterExpressionString);
    }
    @Override
    public void handleRecords(List<KeyspacesStreamsClientRecord> records) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'handleRecords'");
    }

    public Config getConfig() {
        return config;
    }
    
   
}
