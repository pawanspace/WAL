package com.pawanspace.simplewal;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


public class WALReader implements AutoCloseable {
    private final FileChannel channel;
    private volatile boolean isClosed = false;
    
    public WALReader(Path filePath) throws IOException {
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
    }
    
    public WALEntry readEntry() throws IOException {
        if (isClosed) {
            throw new IllegalStateException("WALReader is closed");
        }
        
        // Read length prefix
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        int bytesRead = channel.read(lengthBuffer);
        if (bytesRead == -1) {
            return null; // EOF
        }
        if (bytesRead < 4) {
            throw new IOException("Incomplete length prefix");
        }
        
        lengthBuffer.flip();
        int entryLength = lengthBuffer.getInt();
        
        // Validate entry length
        if (entryLength <= 0) {
            throw new IOException("Invalid entry length: " + entryLength);
        }
        
        // Read the entire entry
        ByteBuffer entryBuffer = ByteBuffer.allocate(entryLength);
        bytesRead = 0;
        while (bytesRead < entryLength) {
            int read = channel.read(entryBuffer);
            if (read == -1) {
                throw new IOException("Unexpected EOF while reading entry");
            }
            bytesRead += read;
        }
        
        entryBuffer.flip();
        
        try {
            // Read entry components
            long sequenceNumber = entryBuffer.getLong();            
            int dataLength = entryBuffer.getInt();
            
            if (dataLength < 0 || dataLength > entryBuffer.remaining()) {
                throw new IOException("Invalid data length: " + dataLength);
            }
            
            byte[] data = new byte[dataLength];
            entryBuffer.get(data);
            
            // Ensure we have enough bytes for checksum
            if (entryBuffer.remaining() < 8) {
                throw new IOException("Buffer underflow: not enough bytes remaining for checksum");
            }
            
            byte[] checksum = new byte[8];
            entryBuffer.get(checksum);
                        
            return WALEntry.create(sequenceNumber, data);
        } catch (BufferUnderflowException e) {
            throw new IOException("Buffer underflow: not enough bytes remaining in entry", e);
        }
    }
    
    @Override
    public void close() throws IOException {
        isClosed = true;
        if (channel.isOpen()) {
            channel.close();
        }
    }
}
