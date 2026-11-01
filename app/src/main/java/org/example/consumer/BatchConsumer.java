
package org.example.consumer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class BatchConsumer {

    public static void main(String[] args) throws IOException {

        if (args.length < 4) {
            System.out.println(
                "Usage: BatchConsumer <topic> <partition> " +
                "<startOffset> <maxMessages>"
            );
            return;
        }

        String topic = args[0];
        int partition = Integer.parseInt(args[1]);
        long startOffset = Long.parseLong(args[2]);
        int maxMessages = Integer.parseInt(args[3]);

        try (
            Socket socket = new Socket("localhost", 9092);
            DataOutputStream output =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream input =
                new DataInputStream(socket.getInputStream())
        ) {

            output.writeUTF("FETCH_BATCH");
            output.writeUTF(topic);
            output.writeInt(partition);
            output.writeLong(startOffset);
            output.writeInt(maxMessages);
            output.flush();

            String response = input.readUTF();

            if (response.equals("BATCH")) {

                int count = input.readInt();

                for (int i = 0; i < count; i++) {
                    String message = input.readUTF();

                    System.out.println(
                        "Offset " + (startOffset + i) +
                        ": " + message
                    );
                }

            } else {
                System.out.println(response);
            }
        }
    }
}
