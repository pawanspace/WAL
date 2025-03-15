package com.pawanspace.simplewal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class WALWriterSmokeTest {
    @TempDir
    Path tempDir;

    @Test
    void smokeTest() throws IOException {
        Path walFile = tempDir.resolve("smoke.wal");
        
        try (WALWriter writer = new WALWriter(walFile)) {
            // Write several entries with different sizes
            writer.writeEntry(WALEntry.create(1L, "small entry".getBytes()));
            writer.writeEntry(WALEntry.create(2L, "medium sized entry with more content".getBytes()));
            
            // Create larger entry
            byte[] largeData = new byte[1024];
            Arrays.fill(largeData, (byte) 'X');
            writer.writeEntry(WALEntry.create(3L, largeData));
        }
    }
}
