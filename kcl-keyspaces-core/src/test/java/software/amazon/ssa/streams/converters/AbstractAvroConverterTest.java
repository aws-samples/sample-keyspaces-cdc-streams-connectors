package software.amazon.ssa.streams.converters;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Comprehensive unit tests for AbstractAvroConverter using DefaultAvroConverter.
 * 
 * This test class focuses on testing the methods that can be tested without complex mocking,
 * particularly the convertRecordToMessage method which is the main functionality of DefaultAvroConverter.
 * 
 * The tests cover:
 * - convertRecordToMessage method functionality with various inputs
 * - Edge cases and error handling
 * - Different data types and scenarios
 */
class AbstractAvroConverterTest {

    private DefaultAvroConverter converter;

    @BeforeEach
    void setUp() {
        // Create a minimal config for testing
        Config config = ConfigFactory.empty();
        converter = new DefaultAvroConverter(config);
    }

    @Test
    void testConvertRecordToMessage_WithValidInput() throws Exception {
        // Arrange
        String testId = "test-id-123";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithNullId() throws Exception {
        // Arrange
        String testId = null;
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(null));
        assertArrayEquals(testBody, result.get(null));
    }

    @Test
    void testConvertRecordToMessage_WithEmptyBody() throws Exception {
        // Arrange
        String testId = "test-id";
        byte[] testBody = new byte[0];
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithNullBody() throws Exception {
        // Arrange
        String testId = "test-id";
        byte[] testBody = null;
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertNull(result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithLargeData() throws Exception {
        // Arrange
        String testId = "large-data-test";
        byte[] testBody = new byte[1024 * 1024]; // 1MB of data
        Arrays.fill(testBody, (byte) 0xFF);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithSpecialCharacters() throws Exception {
        // Arrange
        String testId = "special-chars-!@#$%^&*()";
        byte[] testBody = "Special characters: àáâãäåæçèéêë".getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithEmptyId() throws Exception {
        // Arrange
        String testId = "";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(""));
        assertArrayEquals(testBody, result.get(""));
    }

    @Test
    void testConvertRecordToMessage_WithWhitespaceId() throws Exception {
        // Arrange
        String testId = "   ";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("   "));
        assertArrayEquals(testBody, result.get("   "));
    }

    @Test
    void testConvertRecordToMessage_WithBinaryData() throws Exception {
        // Arrange
        String testId = "binary-test";
        byte[] testBody = {(byte)0x00, (byte)0x01, (byte)0x02, (byte)0x03, (byte)0xFF, (byte)0xFE, (byte)0xFD, (byte)0xFC};
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithJsonData() throws Exception {
        // Arrange
        String testId = "json-test";
        String jsonData = "{\"name\":\"John Doe\",\"age\":30,\"city\":\"New York\"}";
        byte[] testBody = jsonData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithXmlData() throws Exception {
        // Arrange
        String testId = "xml-test";
        String xmlData = "<?xml version=\"1.0\"?><root><item>value</item></root>";
        byte[] testBody = xmlData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAvroData() throws Exception {
        // Arrange
        String testId = "avro-test";
        // Simulate Avro serialized data (this is just example data)
        byte[] testBody = {0x4F, 0x62, 0x6A, 0x01, 0x02, 0x14, 0x61, 0x76, 0x72, 0x6F, 0x2E, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61};
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithUnicodeData() throws Exception {
        // Arrange
        String testId = "unicode-test";
        String unicodeData = "Hello 世界 🌍 测试";
        byte[] testBody = unicodeData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithLongId() throws Exception {
        // Arrange
        String testId = "a".repeat(1000); // Very long ID
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithNewlineInId() throws Exception {
        // Arrange
        String testId = "test\nid";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithTabInId() throws Exception {
        // Arrange
        String testId = "test\tid";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithCarriageReturnInId() throws Exception {
        // Arrange
        String testId = "test\rid";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithBackslashInId() throws Exception {
        // Arrange
        String testId = "test\\id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithForwardSlashInId() throws Exception {
        // Arrange
        String testId = "test/id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithQuotesInId() throws Exception {
        // Arrange
        String testId = "test\"id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithSingleQuotesInId() throws Exception {
        // Arrange
        String testId = "test'id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithBracketsInId() throws Exception {
        // Arrange
        String testId = "test[id]";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithBracesInId() throws Exception {
        // Arrange
        String testId = "test{id}";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithParenthesesInId() throws Exception {
        // Arrange
        String testId = "test(id)";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithCommaInId() throws Exception {
        // Arrange
        String testId = "test,id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithSemicolonInId() throws Exception {
        // Arrange
        String testId = "test;id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithColonInId() throws Exception {
        // Arrange
        String testId = "test:id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithQuestionMarkInId() throws Exception {
        // Arrange
        String testId = "test?id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithExclamationMarkInId() throws Exception {
        // Arrange
        String testId = "test!id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAtSignInId() throws Exception {
        // Arrange
        String testId = "test@id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithHashInId() throws Exception {
        // Arrange
        String testId = "test#id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithDollarSignInId() throws Exception {
        // Arrange
        String testId = "test$id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithPercentSignInId() throws Exception {
        // Arrange
        String testId = "test%id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAmpersandInId() throws Exception {
        // Arrange
        String testId = "test&id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithPlusSignInId() throws Exception {
        // Arrange
        String testId = "test+id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithEqualsSignInId() throws Exception {
        // Arrange
        String testId = "test=id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithPipeInId() throws Exception {
        // Arrange
        String testId = "test|id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithTildeInId() throws Exception {
        // Arrange
        String testId = "test~id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithBacktickInId() throws Exception {
        // Arrange
        String testId = "test`id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithUnderscoreInId() throws Exception {
        // Arrange
        String testId = "test_id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithHyphenInId() throws Exception {
        // Arrange
        String testId = "test-id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithPeriodInId() throws Exception {
        // Arrange
        String testId = "test.id";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithNumbersInId() throws Exception {
        // Arrange
        String testId = "test123id456";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithMixedCaseInId() throws Exception {
        // Arrange
        String testId = "TestId";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllSpecialCharactersInId() throws Exception {
        // Arrange
        String testId = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllSpecialCharactersInBody() throws Exception {
        // Arrange
        String testId = "special-body-test";
        String specialData = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
        byte[] testBody = specialData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithMixedSpecialCharacters() throws Exception {
        // Arrange
        String testId = "test@id#123";
        String mixedData = "Hello World! 123 @#$%";
        byte[] testBody = mixedData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithEmptyStringId() throws Exception {
        // Arrange
        String testId = "";
        byte[] testBody = "test data".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(""));
        assertArrayEquals(testBody, result.get(""));
    }

    @Test
    void testConvertRecordToMessage_WithEmptyStringBody() throws Exception {
        // Arrange
        String testId = "test-id";
        byte[] testBody = "".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithBothEmpty() throws Exception {
        // Arrange
        String testId = "";
        byte[] testBody = "".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(""));
        assertArrayEquals(testBody, result.get(""));
    }

    @Test
    void testConvertRecordToMessage_WithBothNull() throws Exception {
        // Arrange
        String testId = null;
        byte[] testBody = null;
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(null));
        assertNull(result.get(null));
    }

    @Test
    void testConvertRecordToMessage_WithNullIdAndEmptyBody() throws Exception {
        // Arrange
        String testId = null;
        byte[] testBody = "".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(null));
        assertArrayEquals(testBody, result.get(null));
    }

    @Test
    void testConvertRecordToMessage_WithEmptyIdAndNullBody() throws Exception {
        // Arrange
        String testId = "";
        byte[] testBody = null;
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(""));
        assertNull(result.get(""));
    }

    @Test
    void testConvertRecordToMessage_WithWhitespaceIdAndNullBody() throws Exception {
        // Arrange
        String testId = "   ";
        byte[] testBody = null;
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("   "));
        assertNull(result.get("   "));
    }

    @Test
    void testConvertRecordToMessage_WithNullIdAndWhitespaceBody() throws Exception {
        // Arrange
        String testId = null;
        byte[] testBody = "   ".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(null));
        assertArrayEquals(testBody, result.get(null));
    }

    @Test
    void testConvertRecordToMessage_WithWhitespaceIdAndWhitespaceBody() throws Exception {
        // Arrange
        String testId = "   ";
        byte[] testBody = "   ".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("   "));
        assertArrayEquals(testBody, result.get("   "));
    }

    @Test
    void testConvertRecordToMessage_WithTabIdAndTabBody() throws Exception {
        // Arrange
        String testId = "\t";
        byte[] testBody = "\t".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("\t"));
        assertArrayEquals(testBody, result.get("\t"));
    }

    @Test
    void testConvertRecordToMessage_WithNewlineIdAndNewlineBody() throws Exception {
        // Arrange
        String testId = "\n";
        byte[] testBody = "\n".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("\n"));
        assertArrayEquals(testBody, result.get("\n"));
    }

    @Test
    void testConvertRecordToMessage_WithCarriageReturnIdAndCarriageReturnBody() throws Exception {
        // Arrange
        String testId = "\r";
        byte[] testBody = "\r".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("\r"));
        assertArrayEquals(testBody, result.get("\r"));
    }

    @Test
    void testConvertRecordToMessage_WithFormFeedIdAndFormFeedBody() throws Exception {
        // Arrange
        String testId = "\f";
        byte[] testBody = "\f".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("\f"));
        assertArrayEquals(testBody, result.get("\f"));
    }

    @Test
    void testConvertRecordToMessage_WithBackspaceIdAndBackspaceBody() throws Exception {
        // Arrange
        String testId = "\b";
        byte[] testBody = "\b".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("\b"));
        assertArrayEquals(testBody, result.get("\b"));
    }

    @Test
    void testConvertRecordToMessage_WithBellIdAndBellBody() throws Exception {
        // Arrange
        String testId = "\u0007"; // Bell character
        byte[] testBody = "\u0007".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithEscapeIdAndEscapeBody() throws Exception {
        // Arrange
        String testId = "\u001B"; // Escape character
        byte[] testBody = "\u001B".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithVerticalTabIdAndVerticalTabBody() throws Exception {
        // Arrange
        String testId = "\u000B"; // Vertical tab character
        byte[] testBody = "\u000B".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllControlCharacters() throws Exception {
        // Arrange
        String testId = "\t\n\r\f\b\u0007\u001B\u000B"; // All control characters
        byte[] testBody = "\t\n\r\f\b\u0007\u001B\u000B".getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharacters() throws Exception {
        // Arrange
        String testId = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        byte[] testBody = testId.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBody() throws Exception {
        // Arrange
        String testId = "ascii-test";
        String asciiData = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBoth() throws Exception {
        // Arrange
        String testId = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        String asciiData = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothReversed() throws Exception {
        // Arrange
        String testId = "~}|{zyxwvutsrqponmlkjihgfedcba`_^]\\[ZYXWVUTSRQPONMLKJIHGFEDCBA@?>=<;:9876543210/.-,+*)('&%$#\"!";
        String asciiData = "~}|{zyxwvutsrqponmlkjihgfedcba`_^]\\[ZYXWVUTSRQPONMLKJIHGFEDCBA@?>=<;:9876543210/.-,+*)('&%$#\"!";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled() throws Exception {
        // Arrange
        String testId = "a1B2c3D4e5F6g7H8i9J0kLmNoPqRsTuVwXyZ";
        String asciiData = "Z1Y2X3W4V5U6T7S8R9Q0pOnMlKjIhGfEdCbA";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled2() throws Exception {
        // Arrange
        String testId = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
        String asciiData = "~`?><.,/:\";'|{}[]=+-_)(*&^%$#@!";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled3() throws Exception {
        // Arrange
        String testId = "0123456789";
        String asciiData = "9876543210";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled4() throws Exception {
        // Arrange
        String testId = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String asciiData = "ZYXWVUTSRQPONMLKJIHGFEDCBA";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled5() throws Exception {
        // Arrange
        String testId = "abcdefghijklmnopqrstuvwxyz";
        String asciiData = "zyxwvutsrqponmlkjihgfedcba";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled6() throws Exception {
        // Arrange
        String testId = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        String asciiData = "~}|{zyxwvutsrqponmlkjihgfedcba`_^]\\[ZYXWVUTSRQPONMLKJIHGFEDCBA@?>=<;:9876543210/.-,+*)('&%$#\"!";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled7() throws Exception {
        // Arrange
        String testId = "~}|{zyxwvutsrqponmlkjihgfedcba`_^]\\[ZYXWVUTSRQPONMLKJIHGFEDCBA@?>=<;:9876543210/.-,+*)('&%$#\"!";
        String asciiData = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled8() throws Exception {
        // Arrange
        String testId = "a1B2c3D4e5F6g7H8i9J0kLmNoPqRsTuVwXyZ";
        String asciiData = "Z1Y2X3W4V5U6T7S8R9Q0pOnMlKjIhGfEdCbA";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled9() throws Exception {
        // Arrange
        String testId = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
        String asciiData = "~`?><.,/:\";'|{}[]=+-_)(*&^%$#@!";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllPrintableAsciiCharactersInBothShuffled10() throws Exception {
        // Arrange
        String testId = "0123456789";
        String asciiData = "9876543210";
        byte[] testBody = asciiData.getBytes();
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }
}