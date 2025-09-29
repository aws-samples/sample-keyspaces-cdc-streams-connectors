package software.amazon.ssa.streams.connector.sqs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mockStatic;

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

    // ==================== NEW COMPREHENSIVE TESTS ====================

    @Test
    void testCreateMessageBody_FullFormat() throws Exception {
        // Create a spy to test createMessageBody method
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create a mock record
        software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord mockRecord = createMockRecord("test-123");
        
        // Mock the record's getRecord() method and related methods
        software.amazon.awssdk.services.keyspacesstreams.model.Record mockRecordData = mock(software.amazon.awssdk.services.keyspacesstreams.model.Record.class);
        when(mockRecord.getRecord()).thenReturn(mockRecordData);
        when(mockRecord.sequenceNumber()).thenReturn("test-123");
        
        // Mock newImage and oldImage
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockNewImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockOldImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        when(mockRecordData.newImage()).thenReturn(mockNewImage);
        when(mockRecordData.oldImage()).thenReturn(mockOldImage);
        
        // Mock valueCells to return empty map
        when(mockNewImage.valueCells()).thenReturn(new HashMap<>());
        when(mockOldImage.valueCells()).thenReturn(new HashMap<>());
        
        // Call createMessageBody (StreamHelpers.getOperationType will be called with the mock record)
        com.fasterxml.jackson.databind.node.ObjectNode result = spyMapper.createMessageBody(mockRecord);
        
        // Verify the result structure
        assertNotNull(result);
        assertTrue(result.has("metadata"));
        assertTrue(result.has("newImage"));
        assertTrue(result.has("oldImage"));
        
        // Verify metadata content
        com.fasterxml.jackson.databind.node.ObjectNode metadata = (com.fasterxml.jackson.databind.node.ObjectNode) result.get("metadata");
        assertEquals("test_keyspace", metadata.get("keyspace").asText());
        assertEquals("test_table", metadata.get("table").asText());
        // Note: operation type will depend on the actual StreamHelpers.getOperationType implementation
        assertTrue(metadata.has("operation"));
        assertEquals("test-123", metadata.get("sequenceNumber").asText());
        assertTrue(metadata.has("timestamp"));
    }

    @Test
    void testCreateMessageBody_NewImageFormat() throws Exception {
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
        
        SQSTargetMapper spyMapper = spy(newImageMapper);
        
        // Create a mock record
        software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord mockRecord = createMockRecord("test-123");
        software.amazon.awssdk.services.keyspacesstreams.model.Record mockRecordData = mock(software.amazon.awssdk.services.keyspacesstreams.model.Record.class);
        when(mockRecord.getRecord()).thenReturn(mockRecordData);
        when(mockRecord.sequenceNumber()).thenReturn("test-123");
        
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockNewImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        when(mockRecordData.newImage()).thenReturn(mockNewImage);
        when(mockRecordData.oldImage()).thenReturn(null);
        when(mockNewImage.valueCells()).thenReturn(new HashMap<>());
        
        // Call createMessageBody
        com.fasterxml.jackson.databind.node.ObjectNode result = spyMapper.createMessageBody(mockRecord);
        
        // Verify the result structure
        assertNotNull(result);
        assertTrue(result.has("metadata"));
        assertTrue(result.has("newImage"));
        assertFalse(result.has("oldImage")); // Should not have oldImage for new-image format
    }

    @Test
    void testCreateMessageBody_OldImageFormat() throws Exception {
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
        
        SQSTargetMapper spyMapper = spy(oldImageMapper);
        
        // Create a mock record
        software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord mockRecord = createMockRecord("test-123");
        software.amazon.awssdk.services.keyspacesstreams.model.Record mockRecordData = mock(software.amazon.awssdk.services.keyspacesstreams.model.Record.class);
        when(mockRecord.getRecord()).thenReturn(mockRecordData);
        when(mockRecord.sequenceNumber()).thenReturn("test-123");
        
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockOldImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        when(mockRecordData.newImage()).thenReturn(null);
        when(mockRecordData.oldImage()).thenReturn(mockOldImage);
        when(mockOldImage.valueCells()).thenReturn(new HashMap<>());
        
        // Call createMessageBody
        com.fasterxml.jackson.databind.node.ObjectNode result = spyMapper.createMessageBody(mockRecord);
        
        // Verify the result structure
        assertNotNull(result);
        assertTrue(result.has("metadata"));
        assertTrue(result.has("oldImage"));
        assertFalse(result.has("newImage")); // Should not have newImage for old-image format
    }

    @Test
    void testCreateMessageBody_WithoutMetadata() throws Exception {
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
        
        SQSTargetMapper spyMapper = spy(noMetadataMapper);
        
        // Create a mock record
        software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord mockRecord = createMockRecord("test-123");
        software.amazon.awssdk.services.keyspacesstreams.model.Record mockRecordData = mock(software.amazon.awssdk.services.keyspacesstreams.model.Record.class);
        when(mockRecord.getRecord()).thenReturn(mockRecordData);
        // Note: sequenceNumber() is not used when includeMetadata is false
        
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockNewImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockOldImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        when(mockRecordData.newImage()).thenReturn(mockNewImage);
        when(mockRecordData.oldImage()).thenReturn(mockOldImage);
        when(mockNewImage.valueCells()).thenReturn(new HashMap<>());
        when(mockOldImage.valueCells()).thenReturn(new HashMap<>());
        
        // Call createMessageBody
        com.fasterxml.jackson.databind.node.ObjectNode result = spyMapper.createMessageBody(mockRecord);
        
        // Verify the result structure
        assertNotNull(result);
        assertFalse(result.has("metadata")); // Should not have metadata
        assertTrue(result.has("newImage"));
        assertTrue(result.has("oldImage"));
    }

    @Test
    void testCreateMessageBody_UnsupportedFormat() throws Exception {
        // Create mapper with unsupported format
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "unsupported-format");
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config unsupportedConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper unsupportedMapper = new SQSTargetMapper(unsupportedConfig);
        unsupportedMapper.initialize();
        
        SQSTargetMapper spyMapper = spy(unsupportedMapper);
        
        // Create a mock record
        software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord mockRecord = createMockRecord("test-123");
        software.amazon.awssdk.services.keyspacesstreams.model.Record mockRecordData = mock(software.amazon.awssdk.services.keyspacesstreams.model.Record.class);
        when(mockRecord.getRecord()).thenReturn(mockRecordData);
        when(mockRecord.sequenceNumber()).thenReturn("test-123");
        
        // Call createMessageBody and expect exception
        assertThrows(IllegalArgumentException.class, () -> {
            spyMapper.createMessageBody(mockRecord);
        });
    }

    @Test
    void testExtractFieldsAsJson_WithFields() throws Exception {
        // Create a mock KeyspacesRow with value cells
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockRow = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        Map<String, software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell> valueCells = new HashMap<>();
        
        // Create mock cells
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell mockCell1 = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell.class);
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell mockCell2 = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell.class);
        
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue mockValue1 = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.class);
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue mockValue2 = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.class);
        
        when(mockCell1.value()).thenReturn(mockValue1);
        when(mockCell2.value()).thenReturn(mockValue2);
        when(mockValue1.textT()).thenReturn("test-value-1");
        when(mockValue2.textT()).thenReturn("test-value-2");
        when(mockValue1.type()).thenReturn(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.Type.TEXTT);
        when(mockValue2.type()).thenReturn(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue.Type.TEXTT);
        
        valueCells.put("field1", mockCell1);
        valueCells.put("field2", mockCell2);
        
        when(mockRow.valueCells()).thenReturn(valueCells);
        
        // Test with specific fields to include
        List<String> fieldsToInclude = Arrays.asList("field1", "field2");
        
        com.fasterxml.jackson.databind.JsonNode result = sqsTargetMapper.extractFieldsAsJson(mockRow, fieldsToInclude);
        
        assertNotNull(result);
        assertTrue(result.has("field1"));
        assertTrue(result.has("field2"));
        assertEquals("test-value-1", result.get("field1").asText());
        assertEquals("test-value-2", result.get("field2").asText());
    }

    @Test
    void testExtractFieldsAsJson_WithNullRow() throws Exception {
        List<String> fieldsToInclude = Arrays.asList("field1", "field2");
        
        com.fasterxml.jackson.databind.JsonNode result = sqsTargetMapper.extractFieldsAsJson(null, fieldsToInclude);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testExtractFieldsAsJson_WithEmptyFields() throws Exception {
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockRow = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        when(mockRow.valueCells()).thenReturn(new HashMap<>());
        
        List<String> fieldsToInclude = new ArrayList<>();
        
        com.fasterxml.jackson.databind.JsonNode result = sqsTargetMapper.extractFieldsAsJson(mockRow, fieldsToInclude);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testExtractFieldsAsJson_WithMissingFields() throws Exception {
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockRow = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        Map<String, software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell> valueCells = new HashMap<>();
        
        // Note: We don't need to mock the cell and value since the requested fields don't exist
        // and won't be accessed
        
        when(mockRow.valueCells()).thenReturn(valueCells);
        
        // Request fields that don't exist
        List<String> fieldsToInclude = Arrays.asList("missingField1", "missingField2");
        
        com.fasterxml.jackson.databind.JsonNode result = sqsTargetMapper.extractFieldsAsJson(mockRow, fieldsToInclude);
        
        assertNotNull(result);
        assertTrue(result.isEmpty()); // Should be empty since requested fields don't exist
    }

    @Test
    void testConvertRecordsToBatchRequestEntries_MessageSizeLimit() throws Exception {
        // Create a spy to test the method
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create mock records
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            mockRecords.add(createMockRecord("test-" + i));
        }
        
        // Mock createMessageBody to return large JSON objects
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.node.ObjectNode largeMessage = mapper.createObjectNode();
        largeMessage.put("data", "x".repeat(500000)); // Large message (~500KB)
        
        doReturn(largeMessage).when(spyMapper).createMessageBody(any());
        
        // Call the method
        List<SendMessageBatchRequestEntry> result = spyMapper.convertRecordsToBatchRequestEntries(mockRecords);
        
        // Verify that multiple batch entries are created due to size limits
        assertNotNull(result);
        assertTrue(result.size() > 1); // Should create multiple entries due to size limit
    }

    @Test
    void testConvertRecordsToBatchRequestEntries_SingleMessage() throws Exception {
        // Create a spy to test the method
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create a single mock record
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = Arrays.asList(createMockRecord("test-1"));
        
        // Mock createMessageBody to return small JSON objects
        com.fasterxml.jackson.databind.node.ObjectNode smallMessage = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode();
        smallMessage.put("data", "small");
        
        doReturn(smallMessage).when(spyMapper).createMessageBody(any());
        
        // Call the method
        List<SendMessageBatchRequestEntry> result = spyMapper.convertRecordsToBatchRequestEntries(mockRecords);
        
        // Verify that a single batch entry is created
        assertNotNull(result);
        assertEquals(1, result.size());
        
        SendMessageBatchRequestEntry entry = result.get(0);
        assertNotNull(entry.id());
        assertNotNull(entry.messageBody());
        assertEquals(0, entry.delaySeconds()); // Should use configured delay
    }

    @Test
    void testSendBatchMessage_Success() throws Exception {
        // Create a spy to test the method
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Mock SQSService.getInstance() static method
        SQSService mockSqsService = mock(SQSService.class);
        try (var mockedStatic = mockStatic(SQSService.class)) {
            mockedStatic.when(() -> SQSService.getInstance(anyString())).thenReturn(mockSqsService);
            
            // Create a successful response
            SendMessageBatchResponse successResponse = SendMessageBatchResponse.builder()
                .successful(Arrays.asList(
                    SendMessageBatchResultEntry.builder().id("1").messageId("msg-1").build(),
                    SendMessageBatchResultEntry.builder().id("2").messageId("msg-2").build()
                ))
                .failed(new ArrayList<>())
                .build();
            
            when(mockSqsService.sendBatchRequest(any())).thenReturn(successResponse);
            
            // Create a batch request
            SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(Arrays.asList(
                    SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build(),
                    SendMessageBatchRequestEntry.builder().id("2").messageBody("test2").build()
                ))
                .build();
            
            // Call the method - should not throw exception
            assertDoesNotThrow(() -> spyMapper.sendBatchMessage(batchRequest));
            
            // Verify SQS service was called
            verify(mockSqsService, times(1)).sendBatchRequest(batchRequest);
        }
    }

    @Test
    void testSendBatchMessage_PartialFailure() throws Exception {
        // Create a spy to test the method
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Mock SQSService.getInstance() static method
        SQSService mockSqsService = mock(SQSService.class);
        try (var mockedStatic = mockStatic(SQSService.class)) {
            mockedStatic.when(() -> SQSService.getInstance(anyString())).thenReturn(mockSqsService);
            
            // Create a partial failure response
            SendMessageBatchResponse partialFailureResponse = SendMessageBatchResponse.builder()
                .successful(Arrays.asList(
                    SendMessageBatchResultEntry.builder().id("1").messageId("msg-1").build()
                ))
                .failed(Arrays.asList(
                    BatchResultErrorEntry.builder().id("2").code("InvalidParameter").message("Invalid message").build()
                ))
                .build();
            
            when(mockSqsService.sendBatchRequest(any())).thenReturn(partialFailureResponse);
            
            // Create a batch request
            SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(Arrays.asList(
                    SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build(),
                    SendMessageBatchRequestEntry.builder().id("2").messageBody("test2").build()
                ))
                .build();
            
            // Call the method and expect PartialFailureException
            software.amazon.ssa.streams.exception.PartialFailureException exception = assertThrows(
                software.amazon.ssa.streams.exception.PartialFailureException.class,
                () -> spyMapper.sendBatchMessage(batchRequest)
            );
            
            // Verify exception details
            assertEquals(2, exception.getTotalItems());
            assertEquals(1, exception.getFailedItems());
            assertFalse(exception.getErrorMessages().isEmpty());
        }
    }

    @Test
    void testSendBatchMessage_AllItemsFailure() throws Exception {
        // Create a spy to test the method
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Mock SQSService.getInstance() static method
        SQSService mockSqsService = mock(SQSService.class);
        try (var mockedStatic = mockStatic(SQSService.class)) {
            mockedStatic.when(() -> SQSService.getInstance(anyString())).thenReturn(mockSqsService);
            
            // Create an all items failure response
            SendMessageBatchResponse allFailureResponse = SendMessageBatchResponse.builder()
                .successful(new ArrayList<>())
                .failed(Arrays.asList(
                    BatchResultErrorEntry.builder().id("1").code("InvalidParameter").message("Invalid message 1").build(),
                    BatchResultErrorEntry.builder().id("2").code("InvalidParameter").message("Invalid message 2").build()
                ))
                .build();
            
            when(mockSqsService.sendBatchRequest(any())).thenReturn(allFailureResponse);
            
            // Create a batch request
            SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(Arrays.asList(
                    SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build(),
                    SendMessageBatchRequestEntry.builder().id("2").messageBody("test2").build()
                ))
                .build();
            
            // Call the method and expect AllItemsFailureException
            software.amazon.ssa.streams.exception.AllItemsFailureException exception = assertThrows(
                software.amazon.ssa.streams.exception.AllItemsFailureException.class,
                () -> spyMapper.sendBatchMessage(batchRequest)
            );
            
            // Verify exception details
            assertEquals(SQSTargetMapper.class.getName(), exception.getConnectorName());
            assertEquals(2, exception.getTotalItems());
            assertFalse(exception.getErrorMessages().isEmpty());
        }
    }

    @Test
    void testConvertBatchEntriesToBatchRequest_LargeNumberOfEntries() throws Exception {
        // Create 25 entries to test multiple batches
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            entries.add(SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody("test" + i)
                .build());
        }
        
        List<SendMessageBatchRequest> batchRequests = sqsTargetMapper.convertBatchEntriesToBatchRequest(entries);
        
        assertNotNull(batchRequests);
        assertEquals(3, batchRequests.size()); // Should create 3 batches: 10 + 10 + 5
        
        // Verify first batch has 10 entries
        assertEquals(10, batchRequests.get(0).entries().size());
        // Verify second batch has 10 entries
        assertEquals(10, batchRequests.get(1).entries().size());
        // Verify third batch has 5 entries
        assertEquals(5, batchRequests.get(2).entries().size());
        
        // Verify all batches have correct queue URL
        for (SendMessageBatchRequest batchRequest : batchRequests) {
            assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue", batchRequest.queueUrl());
        }
    }

    @Test
    void testConvertBatchEntriesToBatchRequest_ExactlyTwentyEntries() throws Exception {
        // Create exactly 20 entries (should create 2 batches of 10 each)
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            entries.add(SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody("test" + i)
                .build());
        }
        
        List<SendMessageBatchRequest> batchRequests = sqsTargetMapper.convertBatchEntriesToBatchRequest(entries);
        
        assertNotNull(batchRequests);
        assertEquals(2, batchRequests.size());
        assertEquals(10, batchRequests.get(0).entries().size());
        assertEquals(10, batchRequests.get(1).entries().size());
    }

    @Test
    void testConvertBatchEntriesToBatchRequest_OneEntry() throws Exception {
        // Create exactly 1 entry
        List<SendMessageBatchRequestEntry> entries = Arrays.asList(
            SendMessageBatchRequestEntry.builder()
                .id("1")
                .messageBody("test1")
                .build()
        );
        
        List<SendMessageBatchRequest> batchRequests = sqsTargetMapper.convertBatchEntriesToBatchRequest(entries);
        
        assertNotNull(batchRequests);
        assertEquals(1, batchRequests.size());
        assertEquals(1, batchRequests.get(0).entries().size());
        assertEquals("1", batchRequests.get(0).entries().get(0).id());
        assertEquals("test1", batchRequests.get(0).entries().get(0).messageBody());
    }

    @Test
    void testConvertBatchEntriesToBatchRequest_NullList() throws Exception {
        // Test with null list
        assertThrows(NullPointerException.class, () -> {
            sqsTargetMapper.convertBatchEntriesToBatchRequest(null);
        });
    }

    @Test
    void testHandleRecords_WithExceptionInProcessing() throws Exception {
        // Create a spy to test exception handling
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create mock records
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = Arrays.asList(createMockRecord("1"));
        
        // Mock convertRecordsToBatchRequestEntries to throw exception
        doThrow(new RuntimeException("Processing error")).when(spyMapper).convertRecordsToBatchRequestEntries(any());
        
        // Call handleRecords and expect exception
        assertThrows(RuntimeException.class, () -> {
            spyMapper.handleRecords(mockRecords);
        });
        
        // Verify that the method was called
        verify(spyMapper, times(1)).convertRecordsToBatchRequestEntries(mockRecords);
        // Verify that subsequent methods were not called due to exception
        verify(spyMapper, never()).convertBatchEntriesToBatchRequest(any());
        verify(spyMapper, never()).sendBatchMessage(any());
    }

    @Test
    void testHandleRecords_WithExceptionInSending() throws Exception {
        // Create a spy to test exception handling
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create mock records
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = Arrays.asList(createMockRecord("1"));
        
        // Mock the processing methods to succeed
        doReturn(Arrays.asList(
            SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build()
        )).when(spyMapper).convertRecordsToBatchRequestEntries(any());
        
        doReturn(Arrays.asList(
            SendMessageBatchRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
                .entries(Arrays.asList(
                    SendMessageBatchRequestEntry.builder().id("1").messageBody("test1").build()
                ))
                .build()
        )).when(spyMapper).convertBatchEntriesToBatchRequest(any());
        
        // Mock sendBatchMessage to throw exception
        doThrow(new RuntimeException("SQS error")).when(spyMapper).sendBatchMessage(any());
        
        // Call handleRecords and expect exception
        assertThrows(RuntimeException.class, () -> {
            spyMapper.handleRecords(mockRecords);
        });
        
        // Verify that all methods were called
        verify(spyMapper, times(1)).convertRecordsToBatchRequestEntries(mockRecords);
        verify(spyMapper, times(1)).convertBatchEntriesToBatchRequest(any());
        verify(spyMapper, times(1)).sendBatchMessage(any());
    }

    @Test
    void testMessageFormatCaseInsensitive() throws Exception {
        // Test that message format is case insensitive
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.connector.message-format", "FULL"); // Uppercase
        configMap.put("keyspaces-cdc-streams.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.connector.delay-seconds", 0);
        
        Config upperCaseConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper upperCaseMapper = new SQSTargetMapper(upperCaseConfig);
        upperCaseMapper.initialize();
        
        SQSTargetMapper spyMapper = spy(upperCaseMapper);
        
        // Create a mock record
        software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord mockRecord = createMockRecord("test-123");
        software.amazon.awssdk.services.keyspacesstreams.model.Record mockRecordData = mock(software.amazon.awssdk.services.keyspacesstreams.model.Record.class);
        when(mockRecord.getRecord()).thenReturn(mockRecordData);
        when(mockRecord.sequenceNumber()).thenReturn("test-123");
        
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockNewImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockOldImage = mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        when(mockRecordData.newImage()).thenReturn(mockNewImage);
        when(mockRecordData.oldImage()).thenReturn(mockOldImage);
        when(mockNewImage.valueCells()).thenReturn(new HashMap<>());
        when(mockOldImage.valueCells()).thenReturn(new HashMap<>());
        
        // Call createMessageBody - should work with uppercase format
        com.fasterxml.jackson.databind.node.ObjectNode result = spyMapper.createMessageBody(mockRecord);
        
        // Verify the result structure
        assertNotNull(result);
        assertTrue(result.has("metadata"));
        assertTrue(result.has("newImage"));
        assertTrue(result.has("oldImage"));
    }

    @Test
    void testDelaySecondsConfiguration() throws Exception {
        // Test that delay seconds are properly configured and used
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
        
        SQSTargetMapper spyMapper = spy(delayMapper);
        
        // Create a single mock record
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = Arrays.asList(createMockRecord("test-1"));
        
        // Mock createMessageBody to return small JSON objects
        com.fasterxml.jackson.databind.node.ObjectNode smallMessage = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode();
        smallMessage.put("data", "small");
        
        doReturn(smallMessage).when(spyMapper).createMessageBody(any());
        
        // Call the method
        List<SendMessageBatchRequestEntry> result = spyMapper.convertRecordsToBatchRequestEntries(mockRecords);
        
        // Verify that delay seconds are set correctly
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(30, result.get(0).delaySeconds());
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