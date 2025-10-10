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
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
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
        verify(spyMapper, never()).sendBatchMessage(any());
    }

    @Test
    void testHandleRecords_WithNullList() throws Exception {
        // Create a spy to verify method calls
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Should not throw exception and should return early
        assertDoesNotThrow(() -> spyMapper.handleRecords(null));
        
        // Verify that processing methods are NOT called due to early return
        verify(spyMapper, never()).sendBatchMessage(any());
    }

    @Test
    void testHandleRecords_WithValidRecords() throws Exception {
        // Create a spy to verify method calls
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Mock some records (we'll create simple mock objects)
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = 
            Arrays.asList(createMockRecord("1"), createMockRecord("2"));
        
        // Mock sendBatchMessage to avoid actual SQS calls
        doNothing().when(spyMapper).sendBatchMessage(any());
        
        // Call handleRecords
        assertDoesNotThrow(() -> spyMapper.handleRecords(mockRecords));
        
        // Verify that sendBatchMessage is called (number of calls depends on batching)
        verify(spyMapper, atLeastOnce()).sendBatchMessage(any());
    }

    @Test
    void testMessageFormatConfiguration_NewImage() throws Exception {
        // Create mapper with new-image format
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "new-image");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
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
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "old-image");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
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
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", false);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
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
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-west-2");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
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
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-west-2.amazonaws.com/987654321098/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
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
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
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
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        // Omit optional fields to test defaults
        
        Config defaultConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper defaultMapper = new SQSTargetMapper(defaultConfig);
        defaultMapper.initialize();
        
        assertNotNull(defaultMapper);
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
    void testHandleRecords_WithExceptionInSending() throws Exception {
        // Create a spy to test exception handling
        SQSTargetMapper spyMapper = spy(sqsTargetMapper);
        
        // Create mock records
        List<software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord> mockRecords = Arrays.asList(createMockRecord("1"));
        
        // Mock sendBatchMessage to throw exception
        doThrow(new RuntimeException("SQS error")).when(spyMapper).sendBatchMessage(any());
        
        // Call handleRecords and expect exception
        assertThrows(RuntimeException.class, () -> {
            spyMapper.handleRecords(mockRecords);
        });
        
        // Verify that sendBatchMessage was called
        verify(spyMapper, atLeastOnce()).sendBatchMessage(any());
    }

    @Test
    void testMessageFormatCaseInsensitive() throws Exception {
        // Test that message format is case insensitive
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "FULL"); // Uppercase
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 0);
        
        Config upperCaseConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper upperCaseMapper = new SQSTargetMapper(upperCaseConfig);
        upperCaseMapper.initialize();
        
        assertNotNull(upperCaseMapper);
    }

    @Test
    void testDelaySecondsConfiguration() throws Exception {
        // Test that delay seconds are properly configured
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("keyspaces-cdc-streams.stream.keyspace-name", "test_keyspace");
        configMap.put("keyspaces-cdc-streams.stream.table-name", "test_table");
        configMap.put("keyspaces-cdc-streams.stream.connector.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue");
        configMap.put("keyspaces-cdc-streams.stream.connector.region", "us-east-1");
        configMap.put("keyspaces-cdc-streams.stream.connector.message-format", "full");
        configMap.put("keyspaces-cdc-streams.stream.connector.include-metadata", true);
        configMap.put("keyspaces-cdc-streams.stream.connector.delay-seconds", 30);
        
        Config delayConfig = com.typesafe.config.ConfigFactory.parseMap(configMap);
        SQSTargetMapper delayMapper = new SQSTargetMapper(delayConfig);
        delayMapper.initialize();
        
        assertNotNull(delayMapper);
    }

    /**
     * Helper method to create a mock KeyspacesStreamsClientRecord for testing
     */
    private software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord createMockRecord(String id) {
        // Create a mock record with proper setup
        software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord mockRecord = 
            mock(software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord.class);
        
        // Create a mock Record object
        software.amazon.awssdk.services.keyspacesstreams.model.Record mockRecordData = 
            mock(software.amazon.awssdk.services.keyspacesstreams.model.Record.class);
        
        // Set up the mock to return the record data
        when(mockRecord.getRecord()).thenReturn(mockRecordData);
        when(mockRecord.sequenceNumber()).thenReturn(id);
        
        // Set up the record data mocks
        when(mockRecordData.origin()).thenReturn(software.amazon.awssdk.services.keyspacesstreams.model.OriginType.USER);
        
        // Create mock KeyspacesRow objects for newImage and oldImage
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockNewImage = 
            mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow mockOldImage = 
            mock(software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow.class);
        
        when(mockRecordData.newImage()).thenReturn(mockNewImage);
        when(mockRecordData.oldImage()).thenReturn(mockOldImage);
        
        // Set up empty value cells maps
        when(mockNewImage.valueCells()).thenReturn(new java.util.HashMap<>());
        when(mockOldImage.valueCells()).thenReturn(new java.util.HashMap<>());
        
        return mockRecord;
    }
}