
package org.example.broker;

import org.example.storage.AppendOnlyLog;
import org.example.storage.LogManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
