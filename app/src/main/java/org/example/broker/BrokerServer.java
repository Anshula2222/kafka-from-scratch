
package org.example.broker;

import org.example.storage.AppendOnlyLog;
import org.example.storage.LogManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer {

    private final int port;

    public BrokerServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {

        ExecutorService threadPool =
            Executors.newFixedThreadPool(10);

        try (
            LogManager logManager = new LogManager();
            ServerSocket serverSocket = new ServerSocket(port)
        ) {

            System.out.println("Broker started on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();

                threadPool.submit(() ->
                    handleClient(clientSocket, logManager)
                );
            }

        } finally {
            threadPool.shutdown();
        }
    }

    private void handleClient(
            Socket clientSocket,
            LogManager logManager) {

        try (
            Socket socket = clientSocket;
            DataInputStream input = new DataInputStream(
                socket.getInputStream()
            );
            DataOutputStream output = new DataOutputStream(
                socket.getOutputStream()
            )
        ) {

            String command = input.readUTF();

            switch (command) {

                case "PRODUCE" -> {
                    String topic = input.readUTF();
                    int partition = input.readInt();
                    String message = input.readUTF();

                    AppendOnlyLog log =
                        logManager.getLog(topic, partition);

                    long offset = log.append(message);

                    System.out.println(
                        topic + "-" + partition +
                        " offset=" + offset +
                        " message=" + message
                    );

                    output.writeUTF("ACK: offset=" + offset);
                }

                case "FETCH" -> {
                    String topic = input.readUTF();
                    int partition = input.readInt();
                    long offset = input.readLong();

                    AppendOnlyLog log =
                        logManager.getLog(topic, partition);

                    String message = log.read(offset);

                    if (message != null) {
                        output.writeUTF("FOUND");
                        output.writeUTF(message);
                    } else {
                        output.writeUTF("NOT_FOUND");
                    }
                }

                    case "PRODUCE_BATCH" -> {

                        String topic = input.readUTF();
                        int partition = input.readInt();
                        int count = input.readInt();

                        if (count <= 0 || count > 1000) {
                            output.writeUTF("ERROR: Invalid batch size");
                            break;
                        }

                        List<String> messages = new ArrayList<>();

                        for (int i = 0; i < count; i++) {
                            messages.add(input.readUTF());
                        }

                        AppendOnlyLog log =
                            logManager.getLog(topic, partition);

                        long firstOffset = log.appendBatch(messages);

                        output.writeUTF("ACK_BATCH");
                        output.writeLong(firstOffset);
                        output.writeInt(count);

                        System.out.println(
                            "Stored batch of " + count +
                            " messages in " + topic + "-" + partition +
                            " starting at offset " + firstOffset
                        );
                    }

                    case "FETCH_BATCH" -> {

                        String topic = input.readUTF();
                        int partition = input.readInt();
                        long startOffset = input.readLong();
                        int maxMessages = input.readInt();

                        if (maxMessages <= 0 || maxMessages > 1000
                                || startOffset < 0) {
                            output.writeUTF("ERROR: Invalid fetch parameters");
                            break;
                        }

                        AppendOnlyLog log =
                            logManager.getLog(topic, partition);

                        List<String> messages =
                            log.readBatch(startOffset, maxMessages);

                        output.writeUTF("BATCH");
                        output.writeInt(messages.size());

                        for (String message : messages) {
                            output.writeUTF(message);
                        }
                    }


                default -> {
                    output.writeUTF("ERROR: Unknown command");
                }
            }

            output.flush();

        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Client error: " + e.getMessage());
        }
    }
}
