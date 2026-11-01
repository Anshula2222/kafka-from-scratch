
package org.example.producer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class BatchProducer {

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println(
                "Usage: BatchProducer <topic> <partition> <count>"
            );
            return;
        }

        String topic = args[0];
        int partition = Integer.parseInt(args[1]);
        int count = Integer.parseInt(args[2]);

        if (count <= 0 || count > 1000) {
            throw new IllegalArgumentException(
                "Count must be between 1 and 1000"
            );
        }

        try (
            Socket socket = new Socket("localhost", 9092);
            DataOutputStream output =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream input =
                new DataInputStream(socket.getInputStream())
        ) {

            output.writeUTF("PRODUCE_BATCH");
            output.writeUTF(topic);
            output.writeInt(partition);
            output.writeInt(count);

            for (int i = 0; i < count; i++) {
                output.writeUTF("Batch message " + i);
            }

            output.flush();

            String response = input.readUTF();

            if (response.equals("ACK_BATCH")) {
                long firstOffset = input.readLong();
                int written = input.readInt();

                System.out.println(
                    "Stored " + written +
                    " messages at offsets " +
                    firstOffset + " to " +
                    (firstOffset + written - 1)
                );
            } else {
                System.out.println(response);
            }
        }
    }
}
