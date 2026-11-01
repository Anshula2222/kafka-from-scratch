
package org.example.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

public class AppendOnlyLog implements AutoCloseable {

    private static final int INDEX_INTERVAL = 4;

    private final RandomAccessFile file;

    private final TreeMap<Long, Long> index = new TreeMap<>();

    private long nextOffset;

    public AppendOnlyLog(Path path) throws IOException {

        Path parent = path.toAbsolutePath().getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        file = new RandomAccessFile(path.toFile(), "rw");

        recover();
    }

    private void recover() throws IOException {

        file.seek(0);

        long expectedOffset = 0;

        while (file.getFilePointer() < file.length()) {

            long position = file.getFilePointer();

            long offset = file.readLong();
            file.readUTF();

            if (offset != expectedOffset) {
                throw new IOException(
                    "Unexpected offset " + offset +
                    ", expected " + expectedOffset
                );
            }

            if (offset % INDEX_INTERVAL == 0) {
                index.put(offset, position);
            }

            expectedOffset++;
        }

        nextOffset = expectedOffset;
        file.seek(file.length());
    }

    public synchronized long append(String message)
            throws IOException {

        long offset = nextOffset;

        long position = file.length();

        file.seek(position);
        file.writeLong(offset);
        file.writeUTF(message);

        file.getFD().sync();

        if (offset % INDEX_INTERVAL == 0) {
            index.put(offset, position);
        }

        nextOffset++;

        return offset;
    }

    public synchronized String read(long requestedOffset)
            throws IOException {

        if (requestedOffset < 0 ||
                requestedOffset >= nextOffset) {
            return null;
        }

        Map.Entry<Long, Long> entry =
            index.floorEntry(requestedOffset);

        long startPosition = entry != null
            ? entry.getValue()
            : 0;

        file.seek(startPosition);

        while (file.getFilePointer() < file.length()) {

            long offset = file.readLong();
            String message = file.readUTF();

            if (offset == requestedOffset) {
                return message;
            }

            if (offset > requestedOffset) {
                break;
            }
        }

        return null;
    }

    @Override
    public synchronized void close() throws IOException {
        file.close();
    }
}
