package local;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class WALReaderTest {
    private Path tempFile;
    private WALWriter writer;
    private WALReader reader;
    
    @BeforeEach
    void setUp() throws IOException {
        tempFile = Files.createTempFile("wal", ".log");
        writer = new WALWriter(tempFile);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) writer.close();
        if (reader != null) reader.close();
        Files.deleteIfExists(tempFile);
    }
    
    @Test
    void testReadEntry() throws IOException {
        // Arrange
        byte[] data = "test data".getBytes();
        WALEntry originalEntry = WALEntry.create(1L, data);
        
        // Act
        writer.writeEntry(originalEntry);
        writer.close(); // Close writer before reading
        
        reader = new WALReader(tempFile); // Open reader after writing
        WALEntry readEntry = reader.readEntry();
        
        // Assert
        assertNotNull(readEntry);
        assertEquals(originalEntry.getSequenceNumber(), readEntry.getSequenceNumber());
        assertArrayEquals(originalEntry.getData(), readEntry.getData());
        assertArrayEquals(originalEntry.getChecksum(), readEntry.getChecksum());
    }
    
    @Test
    void testReadMultipleEntries() throws IOException {
        // Arrange
        List<WALEntry> entries = Arrays.asList(
            WALEntry.create(1L, "entry1".getBytes()),
            WALEntry.create(2L, "entry2".getBytes()),
            WALEntry.create(3L, "entry3".getBytes())
        );
        
        // Act
        for (WALEntry entry : entries) {
            writer.writeEntry(entry);
        }        
        writer.close();
        
        reader = new WALReader(tempFile);
        
        // Assert
        for (WALEntry expectedEntry : entries) {
            WALEntry readEntry = reader.readEntry();
            assertNotNull(readEntry);
            assertEquals(expectedEntry.getSequenceNumber(), readEntry.getSequenceNumber());
            assertArrayEquals(expectedEntry.getData(), readEntry.getData());
            assertArrayEquals(expectedEntry.getChecksum(), readEntry.getChecksum());
        }
        
        assertNull(reader.readEntry()); // Should return null at EOF
    }
    
    @Test
    void testReadEmptyFile() throws IOException {
        writer.close();
        reader = new WALReader(tempFile);
        assertNull(reader.readEntry());
    }
    
    @Test
    void testReadCorruptedEntry() throws IOException {
        // Write corrupted data
        try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
            ByteBuffer corrupt = ByteBuffer.allocate(4);
            corrupt.putInt(100); // Write invalid length
            corrupt.flip();
            channel.write(corrupt);
        }
        
        reader = new WALReader(tempFile);
        assertThrows(IOException.class, () -> reader.readEntry());
    }
}
