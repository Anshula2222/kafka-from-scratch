
package org.example.broker;

import org.example.storage.AppendOnlyLog;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

public class BrokerServer {

    private final int port;

    public BrokerServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {

        try (
            AppendOnlyLog log = new AppendOnlyLog(
                Path.of("data", "messages.log")
            );
            ServerSocket serverSocket = new ServerSocket(port)
        ) {
            System.out.println("Broker started on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                handleClient(clientSocket, log);
            }
        }
    }

    private void handleClient(
            Socket clientSocket,
            AppendOnlyLog log) {

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
                    String message = input.readUTF();

                    long offset = log.append(message);

                    System.out.println(
                        "Stored at offset " + offset + ": " + message
                    );

                    output.writeUTF("ACK: offset=" + offset);
                }

                case "FETCH" -> {
                    long offset = input.readLong();

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

        } catch (IOException e) {
            System.err.println("Client error: " + e.getMessage());
        }
    }
}
