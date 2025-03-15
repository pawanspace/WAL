# Simple Write-Ahead Log (WAL) Implementation in Java

A thread-safe, high-performance Write-Ahead Log implementation that provides durable and atomic logging capabilities with automatic segment rotation and management.

This project implements a Write-Ahead Log (WAL) system, a fundamental component in database systems and distributed applications that require durability and atomic operations. The WAL ensures that all modifications are written to disk before they are considered committed, providing crash recovery capabilities and data consistency.

The implementation features automatic segment rotation, concurrent read/write operations with proper synchronization, checksumming for data integrity, and a clean API for appending and reading log entries. It supports configurable segment sizes and provides efficient sequential write and read operations through Java NIO channels.

## Repository Structure
```
.
├── build.gradle.kts              # Gradle build configuration with dependencies and project settings
├── gradle/wrapper/              # Gradle wrapper for consistent build environment
├── src/
│   ├── main/java/com/pawanspace/simplewal/
│   │   ├── WALEntry.java        # Core data structure for log entries with checksumming
│   │   ├── WALManager.java      # Main class managing WAL segments and operations
│   │   ├── WALReader.java       # Handles reading entries from WAL segments
│   │   └── WALWriter.java       # Manages writing entries to WAL segments
│   └── test/java/com/pawanspace/simplewal/
│       ├── WALEntryTest.java    # Unit tests for WALEntry
│       ├── WALManagerTest.java  # Unit tests for WALManager
│       ├── WALReaderTest.java   # Unit tests for WALReader
│       └── WALWriterTest.java   # Unit tests for WALWriter
```

## Usage Instructions
### Prerequisites
- Java Development Kit (JDK) 8 or higher
- Gradle 7.0 or higher (included via wrapper)
- SLF4J compatible logging implementation

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd wal
```

2. Build the project:
```bash
./gradlew build
```

### Quick Start

1. Create a WAL Manager instance:
```java
Path walDirectory = Paths.get("/path/to/wal/directory");
long maxSegmentSize = 1024 * 1024; // 1MB segments
WALManager walManager = new WALManager(walDirectory, maxSegmentSize);
```

2. Write entries to the WAL:
```java
byte[] data = "Sample data".getBytes();
WALEntry entry = walManager.append(data);
System.out.println("Written entry with sequence number: " + entry.getSequenceNumber());
```

3. Read entries from a specific segment:
```java
WALReader reader = walManager.getReader(0); // Read from first segment
WALEntry entry;
while ((entry = reader.readEntry()) != null) {
    System.out.println("Read entry: " + new String(entry.getData()));
}
```

### More Detailed Examples

1. Writing multiple entries with error handling:
```java
try (WALManager walManager = new WALManager(walDirectory, maxSegmentSize)) {
    for (int i = 0; i < 1000; i++) {
        String message = "Message " + i;
        WALEntry entry = walManager.append(message.getBytes());
        // Entry is durably written to disk at this point
    }
} catch (IOException e) {
    // Handle IO errors
}
```

2. Reading from multiple segments:
```java
try (WALManager walManager = new WALManager(walDirectory, maxSegmentSize)) {
    // Read from segment 0
    try (WALReader reader = walManager.getReader(0)) {
        WALEntry entry;
        while ((entry = reader.readEntry()) != null) {
            // Process entry
        }
    }
    
    // Read from segment 1
    try (WALReader reader = walManager.getReader(1)) {
        WALEntry entry;
        while ((entry = reader.readEntry()) != null) {
            // Process entry
        }
    }
}
```

### Troubleshooting

1. File Permission Issues
- Problem: Unable to create WAL directory or files
- Solution: Ensure the application has write permissions to the WAL directory
```bash
chmod 755 /path/to/wal/directory
```

2. Segment Rotation Issues
- Problem: Segments not rotating at expected size
- Solution: Enable debug logging to monitor segment sizes
```java
// Configure SLF4J logging to DEBUG level for the package
```

3. Performance Optimization
- Monitor write latency using system tools like `iostat`
- Use direct buffers for large segments:
```java
-XX:MaxDirectMemorySize=64m
```

## Data Flow

The WAL system processes data through a sequential write path that ensures durability and atomic operations. Entries are written to the current segment file and automatically rotated when size limits are reached.

```ascii
[Client] -> [WALManager] -> [WALWriter] -> [Disk]
                |              |
                v              v
           [WALEntry]     [Checksum]
                |              |
                v              v
           [WALReader] <- [Verification]
```

Key component interactions:
1. WALManager coordinates all operations and manages segment lifecycle
2. WALWriter handles atomic writes to the current segment
3. WALEntry encapsulates data with sequence numbers and checksums
4. WALReader provides sequential access to written entries
5. Checksums ensure data integrity during reads
6. Segment rotation occurs automatically at configured size limits
7. Thread-safety is maintained through ReentrantReadWriteLock