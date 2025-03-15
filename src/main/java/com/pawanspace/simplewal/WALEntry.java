package com.pawanspace.simplewal;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.time.Instant;

public class WALEntry {
    // Getters
    @Getter
    private final long sequenceNumber;
    private final byte[] data;
    @Getter
    private final Instant timestamp;
    private final byte[] checksum;

    private WALEntry(long sequenceNumber, byte[] data, Instant timestamp, byte[] checksum) {
        this.sequenceNumber = sequenceNumber;
        this.data = data.clone(); // Defensive copy
        this.timestamp = timestamp;
        this.checksum = checksum.clone(); // Defensive copy
    }

    public static WALEntry create(long sequenceNumber, byte[] data) {
        byte[] checksum = calculateChecksum(sequenceNumber, data);
        return new WALEntry(sequenceNumber, data, Instant.now(), checksum);
    }

    private static byte[] calculateChecksum(long sequenceNumber, byte[] data) {
        // Using CRC32 for checksum
        ByteBuffer buffer = ByteBuffer.allocate(8 + data.length);
        buffer.putLong(sequenceNumber);
        buffer.put(data);
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(buffer.array());
        return ByteBuffer.allocate(8).putLong(crc32.getValue()).array();
    }

    public byte[] getData() { return data.clone(); }
    public byte[] getChecksum() { return checksum.clone(); }
}
