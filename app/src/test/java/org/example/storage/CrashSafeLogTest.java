
package org.example.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CrashSafeLogTest {

    @TempDir
    Path tempDir;

    @Test
    void recoversAfterPartialWrite() throws Exception {

        Path path = tempDir.resolve("test.log");

        try (CrashSafeLog log = new CrashSafeLog(path)) {
            assertEquals(0, log.append("First"));
            assertEquals(1, log.append("Second"));
        }

        // Simulate a crash during the next record.
        try (RandomAccessFile file =
                new RandomAccessFile(path.toFile(), "rw")) {

            file.seek(file.length());
            file.writeInt(100);
            file.writeLong(2);
            file.writeInt(12345);
            file.write(new byte[]{1, 2, 3});
        }

        // Recovery should remove the incomplete record.
        try (CrashSafeLog recovered = new CrashSafeLog(path)) {
            assertEquals(2, recovered.getNextOffset());
            assertEquals(2, recovered.append("Third"));
        }

        // Verify that recovery remains stable.
        try (CrashSafeLog reopened = new CrashSafeLog(path)) {
            assertEquals(3, reopened.getNextOffset());
        }
    }
}
