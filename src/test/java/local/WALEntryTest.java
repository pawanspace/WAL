package local;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class WALEntryTest {

    @Test
    @DisplayName("Should create WALEntry with valid data")
    void testCreateValidEntry() {
        // Given
        long sequenceNumber = 1L;
        byte[] data = "test data".getBytes();

        // When
        WALEntry entry = WALEntry.create(sequenceNumber, data);

        // Then
        assertNotNull(entry);
        assertEquals(sequenceNumber, entry.getSequenceNumber());
        assertArrayEquals(data, entry.getData());
        assertNotNull(entry.getTimestamp());
        assertNotNull(entry.getChecksum());
    }

    @Test
    @DisplayName("Should create defensive copies of byte arrays")
    void testDefensiveCopies() {
        // Given
        byte[] originalData = "test data".getBytes();
        WALEntry entry = WALEntry.create(1L, originalData);

        // When
        byte[] retrievedData = entry.getData();
        retrievedData[0] = 'X'; // Modify the retrieved data

        // Then
        assertFalse(Arrays.equals(retrievedData, entry.getData()));
        assertArrayEquals(originalData, entry.getData());
    }

    @Test
    @DisplayName("Should generate different checksums for different data")
    void testChecksumDifference() {
        // Given
        WALEntry entry1 = WALEntry.create(1L, "data1".getBytes());
        WALEntry entry2 = WALEntry.create(1L, "data2".getBytes());

        // Then
        assertFalse(Arrays.equals(entry1.getChecksum(), entry2.getChecksum()));
    }

    @Test
    @DisplayName("Should generate different checksums for same data with different sequence numbers")
    void testChecksumDifferenceWithSequence() {
        // Given
        byte[] sameData = "same data".getBytes();
        WALEntry entry1 = WALEntry.create(1L, sameData);
        WALEntry entry2 = WALEntry.create(2L, sameData);

        // Then
        assertFalse(Arrays.equals(entry1.getChecksum(), entry2.getChecksum()));
    }

    @Test
    @DisplayName("Should handle empty data")
    void testEmptyData() {
        // Given
        byte[] emptyData = new byte[0];

        // When
        WALEntry entry = WALEntry.create(1L, emptyData);

        // Then
        assertNotNull(entry);
        assertEquals(0, entry.getData().length);
    }

    @Test
    @DisplayName("Should throw NullPointerException for null data")
    void testNullData() {
        assertThrows(NullPointerException.class, () -> {
            WALEntry.create(1L, null);
        });
    }
}
