
package org.example.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

public class AppendOnlyLog implements AutoCloseable {

    private final RandomAccessFile file;
    private long nextOffset;

    public AppendOnlyLog(Path path) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        file = new RandomAccessFile(path.toFile(), "rw");
        nextOffset = recoverNextOffset();
        file.seek(file.length());
    }

    public synchronized long append(String message) throws IOException {
        long offset = nextOffset;

        file.seek(file.length());
        file.writeLong(offset);
        file.writeUTF(message);
        file.getFD().sync();

        nextOffset++;
        return offset;
    }

    private long recoverNextOffset() throws IOException {
        file.seek(0);

        long next = 0;

        while (file.getFilePointer() < file.length()) {
            long storedOffset = file.readLong();
            file.readUTF();
            next = storedOffset + 1;
        }

        return next;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }


    public synchronized String read(long requestedOffset) throws IOException {

        if (requestedOffset < 0) {
            return null;
        }

        file.seek(0);

        while (file.getFilePointer() < file.length()) {

            long storedOffset = file.readLong();
            String message = file.readUTF();

            if (storedOffset == requestedOffset) {
                return message;
            }
        }

        return null;
    }

}
