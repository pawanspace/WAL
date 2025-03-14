package local;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class WALWriterTest {
    @TempDir
    Path tempDir;
    private Path walFile;
    private WALWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        walFile = tempDir.resolve("test.wal");
        writer = new WALWriter(walFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    void writeEntry_SingleEntry_SuccessfullyWrites() throws IOException {
        // Arrange
        byte[] data = "test data".getBytes();
        WALEntry entry = WALEntry.create(1L, data);

        // Act
        writer.writeEntry(entry);

        // Assert
        assertTrue(walFile.toFile().exists());
        assertTrue(walFile.toFile().length() > 0);
        // 4 (length) + 8 (sequence) + 8 (timestamp) + 4 (data length) + 9 (data) + 8 (checksum) = 41 bytes
        assertEquals(41, writer.getWrittenBytes());
    }

    @Test
    void writeEntry_MultipleEntries_SuccessfullyWrites() throws IOException {
        // Arrange
        byte[] data1 = "first entry".getBytes();
        byte[] data2 = "second entry".getBytes();
        WALEntry entry1 = WALEntry.create(1L, data1);
        WALEntry entry2 = WALEntry.create(2L, data2);

        // Act
        writer.writeEntry(entry1);
        long bytesAfterFirst = writer.getWrittenBytes();
        writer.writeEntry(entry2);
        long bytesAfterSecond = writer.getWrittenBytes();

        // Assert
        assertTrue(walFile.toFile().exists());
        assertTrue(walFile.toFile().length() > 0);
        // First entry: 4 + 8 + 8 + 4 + 10 + 8 = 42 bytes
        assertEquals(42, bytesAfterFirst);
        // Second entry: 42 + (4 + 8 + 8 + 4 + 11 + 8) = 85 bytes
        assertEquals(85, bytesAfterSecond);
    }

    @Test
    void writeEntry_AfterClose_ThrowsException() throws IOException {
        // Arrange
        writer.close();
        byte[] data = "test data".getBytes();
        WALEntry entry = WALEntry.create(1L, data);

        // Act & Assert
        assertThrows(IllegalStateException.class, () -> writer.writeEntry(entry));
    }

    @Test
    void writtenBytes_TracksAccurately() throws IOException {
        // Arrange
        List<byte[]> dataEntries = Arrays.asList(
            "small".getBytes(),
            "medium length".getBytes(),
            "much longer entry here".getBytes()
        );
        long expectedBytes = 0;

        // Act & Assert
        for (int i = 0; i < dataEntries.size(); i++) {
            WALEntry entry = WALEntry.create(i + 1L, dataEntries.get(i));
            writer.writeEntry(entry);
            
            // Calculate expected bytes:
            // 4 (length) + 8 (sequence) + 8 (timestamp) + 4 (data length) + data.length + 8 (checksum)
            expectedBytes += (4 + 8 + 8 + 4 + dataEntries.get(i).length + 8);
            assertEquals(expectedBytes, writer.getWrittenBytes(), 
                "Written bytes mismatch after writing entry " + (i + 1));
        }
    }

    @Test
    void getWrittenBytes_NoWrites_ReturnsZero() {
        // Assert
        assertEquals(0, writer.getWrittenBytes());
    }

    @Test
    void getWrittenBytes_AfterClose_RetainsValue() throws IOException {
        // Arrange
        byte[] data = "test data".getBytes();
        WALEntry entry = WALEntry.create(1L, data);
        writer.writeEntry(entry);
        long bytesBeforeClose = writer.getWrittenBytes();

        // Act
        writer.close();

        // Assert
        assertEquals(bytesBeforeClose, writer.getWrittenBytes());
    }
}
