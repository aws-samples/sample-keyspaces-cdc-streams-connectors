package software.amazon.ssa.streams.connector.sqs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.typesafe.config.Config;

import software.amazon.awssdk.services.sqs.model.*;

/**
 * Unit tests for SQSTargetMapper
 * 
 * These tests focus on testing the core logic and configuration handling
 * with proper mocking to verify method call behavior. The tests use Mockito
 * spies to verify that expensive processing methods are not called when
 * the input is empty or null, ensuring the early return logic works correctly.
 */
@ExtendWith(MockitoExtension.class)
class SQSTargetMapperTest {

    private Config config;
    private SQSTargetMapper sqsTargetMapper;

    @BeforeEach
    void setUp() {
        // Create a simple mock config using a Map
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        config = com.typesafe.config.ConfigFactory.parseMap(configMap);
        sqsTargetMapper = new SQSTargetMapper(config);
        sqsTargetMapper.initialize();
    }

    @Test
    void testConstructor_WithValidConfig() {
        assertNotNull(sqsTargetMapper);
    }

    @Test
    void testHandleRecords_WithEmptyList() throws Exception {
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> emptyRecords = new ArrayList<>();
        
        // Create a spy to verify method calls
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Should not throw exception and should return early
        assertDoesNotThrow(() -> spyMapper.handleRecords(emptyRecords));
        
        // Verify that processing methods are NOT called due to early return
        verify(spyMapper, never()).convertRecordsToBatchRequestEntries(any());
        verify(spyMapper, never()).convertBatchEntriesToBatchRequest(any());
        verify(spyMapper, never()).sendBatchMessage(any());
    }

    @Test
    void testHandleRecords_WithNullList() throws Exception {
        // Create a spy to verify method calls
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Should not throw exception and should return early
        assertDoesNotThrow(() -> spyMapper.handleRecords(null));
        
        // Verify that processing methods are NOT called due to early return
        verify(spyMapper, never()).convertRecordsToBatchRequestEntries(any());
        verify(spyMapper, never()).convertBatchEntriesToBatchRequest(any());
        verify(spyMapper, never()).sendBatchMessage(any());
    }

    @Test
    void testHandleRecords_WithValidRecords() throws Exception {
        // Create a spy to verify method calls
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Mock some records (we'll create simple mock objects)
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = 
            Arrays.asList(createMockRecord("1"), createMockRecord("2"));
        
        // Mock the processing methods to avoid actual SQS calls
        doReturn(Arrays.asList(
            SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build(),
            SendMessageBatchRequestEntry.builder().id("2").messageBody("test2").build()
        )).when(spyMapper).convertRecordsToBatchRequestEntries(any());
        
        doReturn(Arrays.asList(
            SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(Arrays.asList(
                    SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build(),
                    SendMessageBatchRequestEntry.builder().id("2").messageBody("test2").build()
                ))
                .build()
        )).when(spyMapper).convertBatchEntriesToBatchRequest(any());
        
        doNothing().when(spyMapper).sendBatchMessage(any());
        
        // Call handleRecords
        assertDoesNotThrow(() -> spyMapper.handleRecords(mockRecords));
        
        // Verify that all processing methods are called
        verify(spyMapper, times(1)).convertRecordsToBatchRequestEntries(mockRecords);
        verify(spyMapper, times(1)).convertBatchEntriesToBatchRequest(any());
        verify(spyMapper, times(1)).sendBatchMessage(any());
    }


    @Test
    void testConvertBatchEntriesToBatchRequest_SingleBatch() throws Exception {
        List<SendMessageBatchRequestEntry> entries = Arrays.asList(
            SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build(),
            SendMessageBatchRequestEntry.builder().id("2").messageBody("test2").build()
        );
        
        List<SendMessageBatchRequest> batchRequests = sqsTargetMapper.convertBatchEntriesToBatchRequest(entries);
        
        assertNotNull(batchRequests);
        assertEquals(1, batchRequests.size());
        
        SendMessageBatchRequest batchRequest = batchRequests.get(0);
        assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue", batchRequest.queueUrl());
        assertEquals(2, batchRequest.entries().size());
        
        // Verify the entries are correctly mapped
        assertEquals("1", batchRequest.entries().get(0).id());
        assertEquals("test1", batchRequest.entries().get(0).messageBody());
        assertEquals("2", batchRequest.entries().get(1).id());
        assertEquals("test2", batchRequest.entries().get(1).messageBody());
    }

    @Test
    void testConvertBatchEntriesToBatchRequest_MultipleBatches() throws Exception {
        // Create 15 entries to test batching (should create 2 batches: 10 + 5)
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            entries.add(SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody("test" + i)
                .build());
        }
        
        List<SendMessageBatchRequest> batchRequests = sqsTargetMapper.convertBatchEntriesToBatchRequest(entries);
        
        assertNotNull(batchRequests);
        assertEquals(2, batchRequests.size());
        
        // First batch should have 10 entries
        assertEquals(10, batchRequests.get(0).entries().size());
        // Second batch should have 5 entries
        assertEquals(5, batchRequests.get(1).entries().size());
    }

    @Test
    void testConvertBatchEntriesToBatchRequest_EmptyList() throws Exception {
        List<SendMessageBatchRequestEntry> emptyEntries = new ArrayList<>();
        
        List<SendMessageBatchRequest> batchRequests = sqsTargetMapper.convertBatchEntriesToBatchRequest(emptyEntries);
        
        assertNotNull(batchRequests);
        assertEquals(0, batchRequests.size());
    }

    @Test
    void testConvertBatchEntriesToBatchRequest_ExactlyTenEntries() throws Exception {
        // Create exactly 10 entries (should create 1 batch)
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries.add(SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody("test" + i)
                .build());
        }
        
        List<SendMessageBatchRequest> batchRequests = sqsTargetMapper.convertBatchEntriesToBatchRequest(entries);
        
        assertNotNull(batchRequests);
        assertEquals(1, batchRequests.size());
        assertEquals(10, batchRequests.get(0).entries().size());
    }

    @Test
    void testHandleRecords_ExactlyTenEntries_OneBatchCall() throws Exception {
        // Create a spy to verify method calls
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create exactly 10 mock records
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            mockRecords.add(createMockRecord(String.valueOf(i)));
        }
        
        // Mock the processing methods to avoid actual SQS calls
        List<SendMessageBatchRequestEntry> mockEntries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            mockEntries.add(SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody("test" + i)
                .build());
        }
        
        doReturn(mockEntries).when(spyMapper).convertRecordsToBatchRequestEntries(any());
        
        List<SendMessageBatchRequest> mockBatchRequests = Arrays.asList(
            SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(mockEntries)
                .build()
        );
        
        doReturn(mockBatchRequests).when(spyMapper).convertBatchEntriesToBatchRequest(any());
        doNothing().when(spyMapper).sendBatchMessage(any());
        
        // Call handleRecords
        assertDoesNotThrow(() -> spyMapper.handleRecords(mockRecords));
        
        // Verify that sendBatchMessage is called exactly once (one batch for 10 entries)
        verify(spyMapper, times(1)).sendBatchMessage(any());
        verify(spyMapper, times(1)).convertRecordsToBatchRequestEntries(mockRecords);
        verify(spyMapper, times(1)).convertBatchEntriesToBatchRequest(any());
    }

    @Test
    void testHandleRecords_ElevenEntries_TwoBatchCalls() throws Exception {
        // Create a spy to verify method calls
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create exactly 11 mock records (should create 2 batches: 10 + 1)
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            mockRecords.add(createMockRecord(String.valueOf(i)));
        }
        
        // Mock the processing methods to avoid actual SQS calls
        List<SendMessageBatchRequestEntry> mockEntries = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            mockEntries.add(SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody("test" + i)
                .build());
        }
        
        doReturn(mockEntries).when(spyMapper).convertRecordsToBatchRequestEntries(any());
        
        // Mock 2 batch requests (10 entries + 1 entry)
        List<SendMessageBatchRequest> mockBatchRequests = Arrays.asList(
            SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(mockEntries.subList(0, 10)) // First 10 entries
                .build(),
            SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(mockEntries.subList(10, 11)) // Last 1 entry
                .build()
        );
        
        doReturn(mockBatchRequests).when(spyMapper).convertBatchEntriesToBatchRequest(any());
        doNothing().when(spyMapper).sendBatchMessage(any());
        
        // Call handleRecords
        assertDoesNotThrow(() -> spyMapper.handleRecords(mockRecords));
        
        // Verify that sendBatchMessage is called exactly twice (two batches for 11 entries)
        verify(spyMapper, times(2)).sendBatchMessage(any());
        verify(spyMapper, times(1)).convertRecordsToBatchRequestEntries(mockRecords);
        verify(spyMapper, times(1)).convertBatchEntriesToBatchRequest(any());
    }

    @Test
    void testMessageFormatConfiguration_NewImage() throws Exception {
        // Create mapper with new-image format
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "new-image");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config newImageConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper newImageMapper = new SQSTargetMapper(newImageConfig);
        newImageMapper.initialize();
        
        assertNotNull(newImageMapper);
    }

    @Test
    void testMessageFormatConfiguration_OldImage() throws Exception {
        // Create mapper with old-image format
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "old-image");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config oldImageConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper oldImageMapper = new SQSTargetMapper(oldImageConfig);
        oldImageMapper.initialize();
        
        assertNotNull(oldImageMapper);
    }

    @Test
    void testMessageFormatConfiguration_WithoutMetadata() throws Exception {
        // Create mapper without metadata
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", false);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config noMetadataConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper noMetadataMapper = new SQSTargetMapper(noMetadataConfig);
        noMetadataMapper.initialize();
        
        assertNotNull(noMetadataMapper);
    }

    @Test
    void testDelaySecondsConfiguration() throws Exception {
        // Create mapper with delay seconds
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 30);
        
        Config delayConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper delayMapper = new SQSTargetMapper(delayConfig);
        delayMapper.initialize();
        
        assertNotNull(delayMapper);
    }

    @Test
    void testRegionConfiguration() throws Exception {
        // Create mapper with different region
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-west-2");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config regionConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper regionMapper = new SQSTargetMapper(regionConfig);
        regionMapper.initialize();
        
        assertNotNull(regionMapper);
    }

    @Test
    void testQueueUrlConfiguration() throws Exception {
        // Create mapper with different queue URL
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-west-2.amazonaws.com/987654321098/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config queueConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper queueMapper = new SQSTargetMapper(queueConfig);
        queueMapper.initialize();
        
        assertNotNull(queueMapper);
    }

    @Test
    void testKeyspaceAndTableConfiguration() throws Exception {
        // Create mapper with different keyspace and table
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "production_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "production_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config prodConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper prodMapper = new SQSTargetMapper(prodConfig);
        prodMapper.initialize();
        
        assertNotNull(prodMapper);
    }

    @Test
    void testDefaultConfigurationValues() throws Exception {
        // Test with minimal configuration to verify defaults
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        // Omit optional fields to test defaults
        
        Config defaultConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper defaultMapper = new SQSTargetMapper(defaultConfig);
        defaultMapper.initialize();
        
        assertNotNull(defaultMapper);
    }

    /**
     * Helper method to create a mock KeyspacesStreamsClientRecord for testing
     */
    private software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord createMockRecord(String id) {
        // Create a simple mock record - we'll use a minimal implementation
        // Since we can't easily mock the actual AWS SDK class, we'll create a basic mock
        return mock(software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord.class);
    }
}