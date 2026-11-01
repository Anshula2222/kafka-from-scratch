
package org.example.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class LogManager implements AutoCloseable {

    private final Map<String, AppendOnlyLog> logs = new HashMap<>();

    public synchronized AppendOnlyLog getLog(
            String topic, int partition) throws IOException {

        if (!topic.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException("Invalid topic name");
        }

        if (partition < 0 || partition >= 2) {
            throw new IllegalArgumentException(
                "Partition must be 0 or 1"
            );
        }

        String key = topic + "-" + partition;

        if (!logs.containsKey(key)) {
            Path path = Path.of(
                "data", topic, "partition-" + partition + ".log"
            );

            logs.put(key, new AppendOnlyLog(path));

            System.out.println("Opened log: " + path);
        }

        return logs.get(key);
    }

    @Override
    public synchronized void close() throws IOException {
        IOException failure = null;

        for (AppendOnlyLog log : logs.values()) {
            try {
                log.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        logs.clear();

        if (failure != null) {
            throw failure;
        }
    }
}
