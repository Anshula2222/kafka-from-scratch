
package org.example.consumer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Consumer {

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println(
                "Usage: Consumer <topic> <partition> <offset>"
            );
            return;
        }

        String topic = args[0];
        int partition = Integer.parseInt(args[1]);
        long offset = Long.parseLong(args[2]);

        try (
            Socket socket = new Socket("localhost", 9092);
            DataOutputStream output = new DataOutputStream(
                socket.getOutputStream()
            );
            DataInputStream input = new DataInputStream(
                socket.getInputStream()
            )
        ) {

            output.writeUTF("FETCH");
            output.writeUTF(topic);
            output.writeInt(partition);
            output.writeLong(offset);
            output.flush();

            String response = input.readUTF();

            if (response.equals("FOUND")) {
                System.out.println(
                    "Message: " + input.readUTF()
                );
            } else {
                System.out.println(response);
            }
        }
    }
}
