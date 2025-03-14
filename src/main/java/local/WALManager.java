package local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * WALManager is responsible for managing Write-Ahead Logs (WALs).
 * It handles the creation, rotation, and reading of WAL segments.
 */
public class WALManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
    private static final String WAL_FILE_PREFIX = "wal-";
    private static final String WAL_FILE_SUFFIX = ".log";

    private final Path walDirectory;
    private final long maxSegmentSize;
    private final ReentrantReadWriteLock lock;
    private volatile boolean isClosed;

    private WALWriter currentWriter;
    private long currentSegmentNumber;
    private final Map<Long, WALReader> readers;
    private final AtomicLong nextSequenceNumber;

    public WALManager(Path walDirectory, long maxSegmentSize) throws IOException {
        logger.info("Initializing WALManager with directory: {}, maxSegmentSize: {}", walDirectory, maxSegmentSize);
        this.walDirectory = walDirectory;
        this.maxSegmentSize = maxSegmentSize;
        this.lock = new ReentrantReadWriteLock();
        this.readers = new ConcurrentHashMap<>();
        this.nextSequenceNumber = new AtomicLong(0);
        this.isClosed = false;

        Files.createDirectories(walDirectory);
        logger.debug("Created WAL directory: {}", walDirectory);

        initializeFromExistingWALs();
    }

    private void initializeFromExistingWALs() throws IOException {
        logger.info("Initializing from existing WAL files");
        try (Stream<Path> files = Files.list(walDirectory)) {
            Optional<Long> maxSegment = files
                    .filter(p -> p.getFileName().toString().startsWith(WAL_FILE_PREFIX))
                    .map(this::extractSegmentNumber)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .max(Long::compareTo);

            currentSegmentNumber = maxSegment.orElse(-1L) + 1;
            logger.debug("Starting with segment number: {}", currentSegmentNumber);

            createNewSegment();

            long maxSeq = findHighestSequenceNumber();
            nextSequenceNumber.set(maxSeq + 1);
            logger.info("Initialized with highest sequence number: {}", maxSeq);
        }
    }

     /**
     * Extracts the segment number from a WAL file name.
     * WAL file format: wal-000000000000000001.log
     *
     * @param path The path of the WAL file
     * @return Optional containing the segment number if valid, empty Optional otherwise
     */
    private Optional<Long> extractSegmentNumber(Path path) {
        String fileName = path.getFileName().toString();
        
        // Check if the file name matches the WAL file pattern
        if (!fileName.startsWith(WAL_FILE_PREFIX) || !fileName.endsWith(WAL_FILE_SUFFIX)) {
            logger.debug("File {} does not match WAL file pattern", fileName);
            return Optional.empty();
        }

        try {
            // Extract the number portion of the filename
            String numberPart = fileName.substring(
                WAL_FILE_PREFIX.length(),
                fileName.length() - WAL_FILE_SUFFIX.length()
            );
            return Optional.of(Long.parseLong(numberPart));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            logger.warn("Failed to extract segment number from file: {}", fileName, e);
            return Optional.empty();
        }
    }


    private long findHighestSequenceNumber() throws IOException {
        logger.debug("Finding highest sequence number from existing segments");
        long maxSeq = -1;
        try (Stream<Path> files = Files.list(walDirectory)) {
            List<Path> walFiles = files
                    .filter(p -> p.getFileName().toString().startsWith(WAL_FILE_PREFIX))
                    .sorted()
                    .collect(Collectors.toList());

            for (Path walFile : walFiles) {
                try (WALReader reader = new WALReader(walFile)) {
                    WALEntry entry;
                    while ((entry = reader.readEntry()) != null) {
                        maxSeq = Math.max(maxSeq, entry.getSequenceNumber());
                    }
                }
            }
        }
        logger.debug("Found highest sequence number: {}", maxSeq);
        return maxSeq;
    }

    public WALEntry append(byte[] data) throws IOException {
        lock.readLock().lock();
        try {
            checkNotClosed();

            WALEntry entry = WALEntry.create(nextSequenceNumber.getAndIncrement(), data);
            currentWriter.writeEntry(entry);
            logger.trace("Appended entry with sequence number: {}, size: {} bytes",
                    entry.getSequenceNumber(), data.length);

            if (currentWriter.getWrittenBytes() >= maxSegmentSize) {
                lock.readLock().unlock();
                lock.writeLock().lock();
                try {
                    if (currentWriter.getWrittenBytes() >= maxSegmentSize) {
                        logger.info("Rotating segment: current size {} bytes exceeds max size {} bytes",
                                currentWriter.getWrittenBytes(), maxSegmentSize);
                        rotateSegment();
                    }
                } finally {
                    lock.writeLock().unlock();
                    lock.readLock().lock();
                }
            }

            return entry;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void rotateSegment() throws IOException {
        logger.info("Rotating WAL segment from {} to {}", currentSegmentNumber, currentSegmentNumber + 1);
        currentWriter.close();
        createNewSegment();
    }

    private void createNewSegment() throws IOException {
        Path newSegmentPath = walDirectory.resolve(
                String.format("%s%019d%s", WAL_FILE_PREFIX, currentSegmentNumber++, WAL_FILE_SUFFIX));
        logger.info("Creating new WAL segment: {}", newSegmentPath);
        currentWriter = new WALWriter(newSegmentPath);
    }

    public WALReader getReader(long segmentNumber) throws IOException {
        lock.readLock().lock();
        try {
            checkNotClosed();
            logger.debug("Getting reader for segment: {}", segmentNumber);
            return readers.computeIfAbsent(segmentNumber, this::createReader);
        } finally {
            lock.readLock().unlock();
        }
    }

    private WALReader createReader(long segmentNumber) {
        try {
            Path segmentPath = walDirectory.resolve(
                    String.format("%s%019d%s", WAL_FILE_PREFIX, segmentNumber, WAL_FILE_SUFFIX));
            logger.debug("Creating new reader for segment: {}", segmentPath);
            return new WALReader(segmentPath);
        } catch (IOException e) {
            logger.error("Failed to create reader for segment: {}", segmentNumber, e);
            throw new UncheckedIOException(e);
        }
    }

    private void checkNotClosed() {
        if (isClosed) {
            logger.error("Attempted to access closed WALManager");
            throw new IllegalStateException("WALManager is closed");
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (!isClosed) {
                logger.info("Closing WALManager");
                isClosed = true;
                if (currentWriter != null) {
                    currentWriter.close();
                }
                for (WALReader reader : readers.values()) {
                    reader.close();
                }
                readers.clear();
                logger.info("WALManager closed successfully");
            }
        } catch (Exception e) {
            logger.error("Error while closing WALManager", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
