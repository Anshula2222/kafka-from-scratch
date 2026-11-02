
package org.example.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

public class CrashSafeLog implements AutoCloseable {

    private static final int MAGIC = 0x4B465332; // KFS2
    private static final int COMMIT = 0x434D4954; // CMIT
    private static final int INDEX_INTERVAL = 4;
    private static final int MAX_MESSAGE_SIZE = 1024 * 1024;
    private static final int MAX_BATCH_SIZE = 1000;

    private final RandomAccessFile file;
    private final TreeMap<Long, Long> index = new TreeMap<>();

    private long nextOffset;

    public CrashSafeLog(Path path) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);

        file = new RandomAccessFile(path.toFile(), "rw");
        recover();
    }

    public synchronized long append(String message)
            throws IOException {
        return appendBatch(List.of(message));
    }

    public synchronized long appendBatch(List<String> messages)
            throws IOException {

        if (messages == null || messages.isEmpty()
                || messages.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("Invalid batch size");
        }

        List<byte[]> payloads = new ArrayList<>();

        for (String message : messages) {
            if (message == null) {
                throw new IllegalArgumentException("Null message");
            }

            byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > MAX_MESSAGE_SIZE) {
                throw new IllegalArgumentException("Message too large");
            }
            payloads.add(bytes);
        }

        long firstOffset = nextOffset;
        long batchStart = file.length();

        try {
            file.seek(batchStart);

            file.writeInt(MAGIC);
            file.writeLong(firstOffset);
            file.writeInt(payloads.size());

            for (int i = 0; i < payloads.size(); i++) {
                byte[] payload = payloads.get(i);
                long offset = firstOffset + i;

                file.writeInt(payload.length);

                CRC32 crc = new CRC32();
                crc.update(payload);
                file.writeInt((int) crc.getValue());

                file.write(payload);
            }

            file.writeInt(COMMIT);
            file.getFD().sync();

            nextOffset += payloads.size();
            rebuildIndex();

            return firstOffset;

        } catch (IOException e) {
            file.setLength(batchStart);
            file.getFD().sync();
            nextOffset = firstOffset;
            rebuildIndex();
            throw e;
        }
    }

    public synchronized String read(long offset) throws IOException {
        List<String> messages = readBatch(offset, 1);
        return messages.isEmpty() ? null : messages.get(0);
    }

    public synchronized List<String> readBatch(
            long startOffset, int maxMessages) throws IOException {

        if (startOffset < 0 || maxMessages <= 0
                || maxMessages > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("Invalid fetch parameters");
        }

        List<String> result = new ArrayList<>();
        if (startOffset >= nextOffset) return result;

        Map.Entry<Long, Long> entry = index.floorEntry(startOffset);
        long position = entry == null ? 0 : entry.getValue();

        file.seek(position);

        while (file.getFilePointer() < file.length()
                && result.size() < maxMessages) {

            int magic = file.readInt();
            if (magic != MAGIC) throw new IOException("Invalid batch header");

            long firstOffset = file.readLong();
            int count = file.readInt();

            for (int i = 0; i < count; i++) {
                int length = file.readInt();
                int checksum = file.readInt();

                byte[] payload = new byte[length];
                file.readFully(payload);

                long offset = firstOffset + i;
                if (offset >= startOffset
                        && result.size() < maxMessages) {
                    result.add(new String(payload, StandardCharsets.UTF_8));
                }
            }

            int marker = file.readInt();
            if (marker != COMMIT) throw new IOException("Missing commit marker");
        }

        return result;
    }

    private synchronized void recover() throws IOException {

        index.clear();
        file.seek(0);

        long validEnd = 0;
        long expectedOffset = 0;

        while (file.getFilePointer() < file.length()) {
            long batchStart = file.getFilePointer();

            try {
                if (file.length() - batchStart < 16) break;

                int magic = file.readInt();
                long firstOffset = file.readLong();
                int count = file.readInt();

                if (magic != MAGIC || firstOffset != expectedOffset
                        || count <= 0 || count > MAX_BATCH_SIZE) break;

                boolean valid = true;

                for (int i = 0; i < count; i++) {
                    if (file.length() - file.getFilePointer() < 8) {
                        valid = false;
                        break;
                    }

                    int length = file.readInt();
                    int checksum = file.readInt();

                    if (length < 0 || length > MAX_MESSAGE_SIZE
                            || file.length() - file.getFilePointer() < length) {
                        valid = false;
                        break;
                    }

                    byte[] payload = new byte[length];
                    file.readFully(payload);

                    CRC32 crc = new CRC32();
                    crc.update(payload);

                    if ((int) crc.getValue() != checksum) {
                        valid = false;
                        break;
                    }
                }

                if (!valid || file.length() - file.getFilePointer() < 4)
                    break;

                if (file.readInt() != COMMIT) break;

                expectedOffset += count;
                validEnd = file.getFilePointer();

            } catch (IOException e) {
                break;
            }
        }

        if (validEnd < file.length()) {
            System.out.println("Truncating invalid batch tail at " + validEnd);
            file.setLength(validEnd);
            file.getFD().sync();
        }

        nextOffset = expectedOffset;
        rebuildIndex();
    }

    private synchronized void rebuildIndex() throws IOException {

        index.clear();
        file.seek(0);

        while (file.getFilePointer() < file.length()) {
            long batchPosition = file.getFilePointer();

            file.readInt(); // magic
            long firstOffset = file.readLong();
            int count = file.readInt();

            // Index each batch start. Multiple indexed offsets may
            // point to the same batch position.
            for (int i = 0; i < count; i++) {
                long offset = firstOffset + i;
                if (offset % INDEX_INTERVAL == 0) {
                    index.put(offset, batchPosition);
                }

                int length = file.readInt();
                file.readInt(); // checksum
                file.seek(file.getFilePointer() + length);
            }

            file.readInt(); // commit marker
        }

        file.seek(file.length());
    }

    public synchronized long getNextOffset() {
        return nextOffset;
    }

    @Override
    public synchronized void close() throws IOException {
        file.close();
    }
}
