package local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicLong;


public class WALWriter implements AutoCloseable {
    private final FileChannel channel;
    private volatile boolean isClosed = false;
    private final AtomicLong writtenBytes;
    
    public WALWriter(Path filePath) throws IOException {
        this.channel = FileChannel.open(filePath, 
            EnumSet.of(StandardOpenOption.CREATE, 
                      StandardOpenOption.WRITE,
                      StandardOpenOption.APPEND));
        this.writtenBytes = new AtomicLong(0);
    }

    public synchronized void writeEntry(WALEntry entry) throws IOException {
        if (isClosed) {
            throw new IllegalStateException("WALWriter is closed");
        }
        
        // Format: [Length(4)][SequenceNumber(8)][Timestamp(8)][DataLength(4)][Data(N)][Checksum(8)]
        byte[] data = entry.getData();
        int totalLength = 8 + 8 + 4 + data.length + 8; // All fields except length field itself
        
        ByteBuffer buffer = ByteBuffer.allocate(4 + totalLength);
        buffer.putInt(totalLength);
        buffer.putLong(entry.getSequenceNumber());
        buffer.putLong(entry.getTimestamp().toEpochMilli());
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.put(entry.getChecksum());
        
        buffer.flip();
        
        // Ensure atomic write
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.force(true);
        
        // Update written bytes counter
        writtenBytes.addAndGet(4 + totalLength);
    }

    public long getWrittenBytes() {
        return writtenBytes.get();
    }

    @Override
    public synchronized void close() throws IOException {
        isClosed = true;
        if (channel.isOpen()) {
            channel.force(true);
            channel.close();
        }
    }
}

