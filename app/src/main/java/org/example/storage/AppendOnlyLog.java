
package org.example.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;

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

        public synchronized long appendBatch(List<String> messages)
            throws IOException {

        if (messages.isEmpty()) {
            throw new IllegalArgumentException(
                "Batch cannot be empty"
            );
        }

        long firstOffset = nextOffset;
        long batchStartPosition = file.length();

        // Validate all messages before writing anything.
        for (String message : messages) {
            if (message == null) {
                throw new IllegalArgumentException(
                    "Message cannot be null"
                );
            }
        }

        try {
            file.seek(batchStartPosition);

            for (String message : messages) {
                long offset = nextOffset;
                long position = file.getFilePointer();

                file.writeLong(offset);
                file.writeUTF(message);

                if (offset % INDEX_INTERVAL == 0) {
                    index.put(offset, position);
                }

                nextOffset++;
            }

            // One disk synchronization for the whole batch.
            file.getFD().sync();

            return firstOffset;

        } catch (IOException e) {
            // Restore the previous log length and in-memory index.
            file.setLength(batchStartPosition);
            index.tailMap(firstOffset, true).clear();
            nextOffset = firstOffset;

            throw e;
        }
    }

    public synchronized List<String> readBatch(
            long startOffset, int maxMessages) throws IOException {

        if (maxMessages <= 0 || startOffset < 0) {
            throw new IllegalArgumentException(
                "Invalid batch read parameters"
            );
        }

        List<String> results = new ArrayList<>();

        if (startOffset >= nextOffset) {
            return results;
        }

        Map.Entry<Long, Long> entry =
            index.floorEntry(startOffset);

        long startPosition = entry != null
            ? entry.getValue()
            : 0;

        file.seek(startPosition);

        while (file.getFilePointer() < file.length()
                && results.size() < maxMessages) {

            long offset = file.readLong();
            String message = file.readUTF();

            if (offset >= startOffset) {
                results.add(message);
            }
        }

        return results;
    }
}

