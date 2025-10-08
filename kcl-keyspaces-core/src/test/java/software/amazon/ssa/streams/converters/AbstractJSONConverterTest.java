package software.amazon.ssa.streams.converters;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Comprehensive unit tests for AbstractJSONConverter using DefaultJSONConverter.
 * 
 * This test class focuses on testing the methods that can be tested without complex mocking,
 * particularly the convertRecordToMessage method which is the main functionality of DefaultJSONConverter.
 * 
 * The tests cover:
 * - convertRecordToMessage method functionality with various inputs
 * - Edge cases and error handling
 * - Different data types and encoding scenarios
 * - Special characters and Unicode handling
 * - JSON-specific formatting and validation
 */
class AbstractJSONConverterTest {

    private DefaultJSONConverter converter;

    @BeforeEach
    void setUp() {
        // Create a minimal config for testing
        Config config = ConfigFactory.empty();
        converter = new DefaultJSONConverter(config);
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
        assertArrayEquals("test data".getBytes(), result.get(null));
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
        assertArrayEquals("".getBytes(), result.get(testId));
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
        Arrays.fill(testBody, (byte) 'A');
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithSpecialCharacters() throws Exception {
        // Arrange
        String testId = "special-chars-!@#$%^&*()";
        String specialData = "Special characters: àáâãäåæçèéêë";
        byte[] testBody = specialData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(specialData.getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(""));
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
        assertArrayEquals("test data".getBytes(), result.get("   "));
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
        assertArrayEquals(jsonData.getBytes(), result.get(testId));
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
        assertArrayEquals(xmlData.getBytes(), result.get(testId));
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
        assertArrayEquals(unicodeData.getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(testId));
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
        assertArrayEquals(specialData.getBytes(), result.get(testId));
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
        assertArrayEquals(mixedData.getBytes(), result.get(testId));
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
        assertArrayEquals("test data".getBytes(), result.get(""));
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
        assertArrayEquals("".getBytes(), result.get(testId));
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
        assertArrayEquals("".getBytes(), result.get(""));
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
        assertArrayEquals("".getBytes(), result.get(null));
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
        assertArrayEquals("   ".getBytes(), result.get(null));
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
        assertArrayEquals("   ".getBytes(), result.get("   "));
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
        assertArrayEquals("\t".getBytes(), result.get("\t"));
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
        assertArrayEquals("\n".getBytes(), result.get("\n"));
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
        assertArrayEquals("\r".getBytes(), result.get("\r"));
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
        assertArrayEquals("\f".getBytes(), result.get("\f"));
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
        assertArrayEquals("\b".getBytes(), result.get("\b"));
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
        assertArrayEquals("\u0007".getBytes(), result.get(testId));
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
        assertArrayEquals("\u001B".getBytes(), result.get(testId));
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
        assertArrayEquals("\u000B".getBytes(), result.get(testId));
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
        assertArrayEquals("\t\n\r\f\b\u0007\u001B\u000B".getBytes(), result.get(testId));
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
        assertArrayEquals(testId.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
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
        assertArrayEquals(asciiData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithJsonEscapedCharacters() throws Exception {
        // Arrange
        String testId = "json-escaped-test";
        String jsonData = "{\"message\":\"Hello \\\"World\\\" with \\n newlines and \\t tabs\"}";
        byte[] testBody = jsonData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(jsonData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithComplexJsonStructure() throws Exception {
        // Arrange
        String testId = "complex-json-test";
        String jsonData = "{\"users\":[{\"id\":1,\"name\":\"John\",\"email\":\"john@example.com\"},{\"id\":2,\"name\":\"Jane\",\"email\":\"jane@example.com\"}],\"metadata\":{\"total\":2,\"page\":1}}";
        byte[] testBody = jsonData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(jsonData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithNestedJsonStructure() throws Exception {
        // Arrange
        String testId = "nested-json-test";
        String jsonData = "{\"level1\":{\"level2\":{\"level3\":{\"value\":\"deep nested data\"}}}}";
        byte[] testBody = jsonData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(jsonData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithJsonArray() throws Exception {
        // Arrange
        String testId = "json-array-test";
        String jsonData = "[1,2,3,4,5,\"string\",true,false,null]";
        byte[] testBody = jsonData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(jsonData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithJsonPrimitives() throws Exception {
        // Arrange
        String testId = "json-primitives-test";
        String jsonData = "{\"string\":\"hello\",\"number\":42,\"float\":3.14,\"boolean\":true,\"null\":null}";
        byte[] testBody = jsonData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(jsonData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithInvalidJson() throws Exception {
        // Arrange
        String testId = "invalid-json-test";
        String invalidJson = "{\"invalid\": json, missing quotes}";
        byte[] testBody = invalidJson.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(invalidJson.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithEmptyJson() throws Exception {
        // Arrange
        String testId = "empty-json-test";
        String emptyJson = "{}";
        byte[] testBody = emptyJson.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(emptyJson.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithEmptyJsonArray() throws Exception {
        // Arrange
        String testId = "empty-json-array-test";
        String emptyJsonArray = "[]";
        byte[] testBody = emptyJsonArray.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(emptyJsonArray.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithWhitespaceOnlyJson() throws Exception {
        // Arrange
        String testId = "whitespace-json-test";
        String whitespaceJson = "   \n\t\r   ";
        byte[] testBody = whitespaceJson.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(whitespaceJson.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithMixedEncoding() throws Exception {
        // Arrange
        String testId = "mixed-encoding-test";
        String mixedData = "ASCII: Hello, Unicode: 世界, Emoji: 🌍, Special: àáâãäå";
        byte[] testBody = mixedData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(mixedData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithDifferentEncodings() throws Exception {
        // Arrange
        String testId = "encoding-test";
        String testData = "Test data with special chars: àáâãäåæçèéêë";
        
        // Test with UTF-8 encoding
        byte[] testBody = testData.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testData.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithVeryLongString() throws Exception {
        // Arrange
        String testId = "very-long-string-test";
        String longString = "A".repeat(10000); // 10KB string
        byte[] testBody = longString.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(longString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithRepeatedPatterns() throws Exception {
        // Arrange
        String testId = "repeated-patterns-test";
        String pattern = "ABC123";
        String repeatedPattern = pattern.repeat(1000); // Repeat pattern 1000 times
        byte[] testBody = repeatedPattern.getBytes("UTF-8");
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(repeatedPattern.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithBinaryLikeData() throws Exception {
        // Arrange
        String testId = "binary-like-test";
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
    void testConvertRecordToMessage_WithNullBytes() throws Exception {
        // Arrange
        String testId = "null-bytes-test";
        byte[] testBody = {(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00};
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithHighBytes() throws Exception {
        // Arrange
        String testId = "high-bytes-test";
        byte[] testBody = {(byte)0xFF, (byte)0xFE, (byte)0xFD, (byte)0xFC};
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithMixedBytes() throws Exception {
        // Arrange
        String testId = "mixed-bytes-test";
        byte[] testBody = {(byte)0x00, (byte)0x41, (byte)0x42, (byte)0x43, (byte)0xFF, (byte)0xFE, (byte)0x00, (byte)0x01};
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithAllByteValues() throws Exception {
        // Arrange
        String testId = "all-byte-values-test";
        byte[] testBody = new byte[256];
        for (int i = 0; i < 256; i++) {
            testBody[i] = (byte) i;
        }
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(testBody, result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithSingleByte() throws Exception {
        // Arrange
        String testId = "single-byte-test";
        byte[] testBody = {(byte) 0x41}; // 'A'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithTwoBytes() throws Exception {
        // Arrange
        String testId = "two-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42}; // 'AB'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithThreeBytes() throws Exception {
        // Arrange
        String testId = "three-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43}; // 'ABC'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithFourBytes() throws Exception {
        // Arrange
        String testId = "four-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43, (byte) 0x44}; // 'ABCD'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithFiveBytes() throws Exception {
        // Arrange
        String testId = "five-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43, (byte) 0x44, (byte) 0x45}; // 'ABCDE'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithSixBytes() throws Exception {
        // Arrange
        String testId = "six-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43, (byte) 0x44, (byte) 0x45, (byte) 0x46}; // 'ABCDEF'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithSevenBytes() throws Exception {
        // Arrange
        String testId = "seven-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43, (byte) 0x44, (byte) 0x45, (byte) 0x46, (byte) 0x47}; // 'ABCDEFG'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithEightBytes() throws Exception {
        // Arrange
        String testId = "eight-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43, (byte) 0x44, (byte) 0x45, (byte) 0x46, (byte) 0x47, (byte) 0x48}; // 'ABCDEFGH'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithNineBytes() throws Exception {
        // Arrange
        String testId = "nine-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43, (byte) 0x44, (byte) 0x45, (byte) 0x46, (byte) 0x47, (byte) 0x48, (byte) 0x49}; // 'ABCDEFGHI'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }

    @Test
    void testConvertRecordToMessage_WithTenBytes() throws Exception {
        // Arrange
        String testId = "ten-bytes-test";
        byte[] testBody = {(byte) 0x41, (byte) 0x42, (byte) 0x43, (byte) 0x44, (byte) 0x45, (byte) 0x46, (byte) 0x47, (byte) 0x48, (byte) 0x49, (byte) 0x4A}; // 'ABCDEFGHIJ'
        String expectedString = new String(testBody);
        
        // Act
        Map<String, byte[]> result = converter.convertRecordToMessage(testId, testBody);
        
        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(testId));
        assertArrayEquals(expectedString.getBytes(), result.get(testId));
    }
}
