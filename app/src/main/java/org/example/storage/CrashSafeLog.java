
package org.example.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

public class CrashSafeLog implements AutoCloseable {

    private static final int HEADER_SIZE = 16;
    private static final int MAX_MESSAGE_SIZE = 1024 * 1024;

    private final RandomAccessFile file;
    private long nextOffset;

    public CrashSafeLog(Path path) throws IOException {
        Path parent = path.toAbsolutePath().getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        file = new RandomAccessFile(path.toFile(), "rw");
        recover();
    }

    public synchronized long append(String message)
            throws IOException {

        byte[] payload = message.getBytes(StandardCharsets.UTF_8);

        if (payload.length > MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("Message too large");
        }

        long offset = nextOffset;
        long startPosition = file.length();

        CRC32 crc = new CRC32();
        crc.update(payload);

        try {
            file.seek(startPosition);

            file.writeInt(payload.length);
            file.writeLong(offset);
            file.writeInt((int) crc.getValue());
            file.write(payload);

            file.getFD().sync();

            nextOffset++;
            return offset;

        } catch (IOException e) {
            file.setLength(startPosition);
            throw e;
        }
    }

    private void recover() throws IOException {

        file.seek(0);

        long validEnd = 0;
        long expectedOffset = 0;

        while (file.getFilePointer() < file.length()) {

            long recordStart = file.getFilePointer();

            try {
                if (file.length() - recordStart < HEADER_SIZE) {
                    break;
                }

                int length = file.readInt();
                long offset = file.readLong();
                int storedCrc = file.readInt();

                if (length < 0 || length > MAX_MESSAGE_SIZE) {
                    break;
                }

                if (offset != expectedOffset) {
                    break;
                }

                if (file.length() - file.getFilePointer() < length) {
                    break;
                }

                byte[] payload = new byte[length];
                file.readFully(payload);

                CRC32 crc = new CRC32();
                crc.update(payload);

                if ((int) crc.getValue() != storedCrc) {
                    break;
                }

                validEnd = file.getFilePointer();
                expectedOffset++;

            } catch (EOFException e) {
                break;
            }
        }

        if (validEnd < file.length()) {
            System.out.println(
                "Truncating incomplete/corrupt log tail: " +
                (file.length() - validEnd) + " bytes"
            );

            file.setLength(validEnd);
            file.getFD().sync();
        }

        nextOffset = expectedOffset;
        file.seek(validEnd);
    }

    public synchronized long getNextOffset() {
        return nextOffset;
    }

    @Override
    public synchronized void close() throws IOException {
        file.close();
    }
}
