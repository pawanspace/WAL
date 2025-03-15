package com.pawanspace.simplewal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class WALManagerTest {
    @TempDir
    Path tempDir;
    private WALManager walManager;
    private static final long MAX_SEGMENT_SIZE = 1024; // 1KB for testing

    @BeforeEach
    void setUp() throws IOException {
        walManager = new WALManager(tempDir, MAX_SEGMENT_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (walManager != null) {
            walManager.close();
        }
    }

    @Test
    void testExtractSegmentNumber() throws Exception {
        // Using reflection to test private method
        Method extractSegmentNumber = WALManager.class.getDeclaredMethod("extractSegmentNumber", Path.class);
        extractSegmentNumber.setAccessible(true);

        // Valid cases
        assertEquals(
            Optional.of(1L),
            extractSegmentNumber.invoke(walManager, tempDir.resolve("wal-000000000000000001.log"))
        );
        assertEquals(
            Optional.of(999999L),
            extractSegmentNumber.invoke(walManager, tempDir.resolve("wal-000000000000999999.log"))
        );
        assertEquals(
            Optional.of(0L),
            extractSegmentNumber.invoke(walManager, tempDir.resolve("wal-000000000000000000.log"))
        );

        // Invalid cases
        assertEquals(
            Optional.empty(),
            extractSegmentNumber.invoke(walManager, tempDir.resolve("invalid-file.log"))
        );
        assertEquals(
            Optional.empty(),
            extractSegmentNumber.invoke(walManager, tempDir.resolve("wal-invalid.log"))
        );
        assertEquals(
            Optional.empty(),
            extractSegmentNumber.invoke(walManager, tempDir.resolve("wal-.log"))
        );
        assertEquals(
            Optional.empty(),
            extractSegmentNumber.invoke(walManager, tempDir.resolve("random.txt"))
        );
    }

    @Test
    void testSegmentNumberExtraction_WithActualFiles() throws IOException {
        // Create some test WAL files
        Files.createFile(tempDir.resolve("wal-000000000000000001.log"));
        Files.createFile(tempDir.resolve("wal-000000000000000002.log"));
        Files.createFile(tempDir.resolve("invalid-file.log"));

        // Initialize new WALManager to test segment number extraction
        WALManager newManager = new WALManager(tempDir, MAX_SEGMENT_SIZE);
        
        // Write an entry to verify the next segment number was correctly determined
        byte[] data = "test".getBytes();
        newManager.append(data);

        // Verify that files exist
        assertTrue(Files.exists(tempDir.resolve("wal-000000000000000003.log")));
    }

    @Test
    void testSegmentNumberExtraction_EmptyDirectory() throws IOException {
        // Create new manager in empty directory
        WALManager newManager = new WALManager(tempDir, MAX_SEGMENT_SIZE);
        
        // Write an entry to verify the segment number starts at 0
        byte[] data = "test".getBytes();
        newManager.append(data);

        // Verify that first segment file was created
        assertTrue(Files.exists(tempDir.resolve("wal-000000000000000000.log")));
    }

    @Test
    void testSegmentNumberExtraction_WithGaps() throws IOException {
        // Create WAL files with gaps
        Files.createFile(tempDir.resolve("wal-000000000000000001.log"));
        Files.createFile(tempDir.resolve("wal-000000000000000003.log"));
        Files.createFile(tempDir.resolve("wal-000000000000000005.log"));

        // Initialize new WALManager
        WALManager newManager = new WALManager(tempDir, MAX_SEGMENT_SIZE);
        
        // Write an entry to verify the next segment number was correctly determined
        byte[] data = "test".getBytes();
        newManager.append(data);

        // Verify that next file was created with correct segment number
        assertTrue(Files.exists(tempDir.resolve("wal-000000000000000006.log")));
    }
}

