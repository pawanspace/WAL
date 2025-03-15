package com.pawanspace.simplewal;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


import static org.junit.jupiter.api.Assertions.*;


public class WALReaderSmokeTest {
    @Test
    void smokeTestBasicWALOperations() throws IOException {
        Path walFile = Files.createTempFile("smoke", ".wal");
        
        try (WALWriter writer = new WALWriter(walFile)) {
            // Write some entries
            for (int i = 0; i < 1000; i++) {
                String data = "Entry " + i;
                WALEntry entry = WALEntry.create(i, data.getBytes());
                writer.writeEntry(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walFile)) {
            // Read and verify entries
            for (int i = 0; i < 1000; i++) {
                WALEntry entry = reader.readEntry();
                assertNotNull(entry);
                assertEquals(i, entry.getSequenceNumber());
                String data = new String(entry.getData());
                assertEquals("Entry " + i, data);
            }
            
            // Verify EOF
            assertNull(reader.readEntry());
        }
        
        Files.delete(walFile);
    }
}
