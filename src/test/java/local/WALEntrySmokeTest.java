package local;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;


class WALEntrySmokeTest {

    @Test
    @DisplayName("Smoke Test: Basic WALEntry operations")
    void smokeTestBasicOperations() {
        // Create an entry
        long sequenceNumber = 1L;
        byte[] data = "Important transaction data".getBytes();
        WALEntry entry = WALEntry.create(sequenceNumber, data);

        // Verify all components
        assertAll(
            () -> assertEquals(sequenceNumber, entry.getSequenceNumber()),
            () -> assertArrayEquals(data, entry.getData()),
            () -> assertNotNull(entry.getTimestamp()),
            () -> assertTrue(entry.getTimestamp().isBefore(Instant.now())),
            () -> assertNotNull(entry.getChecksum()),
            () -> assertTrue(entry.getChecksum().length > 0)
        );

        // Verify data immutability
        byte[] retrievedData = entry.getData();
        retrievedData[0] = 'X';
        assertArrayEquals(data, entry.getData());
    }

    @Test
    @DisplayName("Smoke Test: Multiple entries creation and verification")
    void smokeTestMultipleEntries() {
        // Create multiple entries in quick succession
        WALEntry entry1 = WALEntry.create(1L, "First entry".getBytes());
        WALEntry entry2 = WALEntry.create(2L, "Second entry".getBytes());
        WALEntry entry3 = WALEntry.create(3L, "Third entry".getBytes());

        // Verify sequence numbers are maintained
        assertAll(
            () -> assertEquals(1L, entry1.getSequenceNumber()),
            () -> assertEquals(2L, entry2.getSequenceNumber()),
            () -> assertEquals(3L, entry3.getSequenceNumber())
        );

        // Verify timestamps are in correct order
        assertAll(
            () -> assertTrue(entry1.getTimestamp().compareTo(entry2.getTimestamp()) <= 0),
            () -> assertTrue(entry2.getTimestamp().compareTo(entry3.getTimestamp()) <= 0)
        );

        // Verify checksums are different
        assertAll(
            () -> assertFalse(Arrays.equals(entry1.getChecksum(), entry2.getChecksum())),
            () -> assertFalse(Arrays.equals(entry2.getChecksum(), entry3.getChecksum())),
            () -> assertFalse(Arrays.equals(entry1.getChecksum(), entry3.getChecksum()))
        );
    }
}
