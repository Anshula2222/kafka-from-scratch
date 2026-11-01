
package org.example.producer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Producer {

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println(
                "Usage: Producer <topic> <partition> <message>"
            );
            return;
        }

        String topic = args[0];
        int partition = Integer.parseInt(args[1]);
        String message = String.join(
            " ", java.util.Arrays.copyOfRange(args, 2, args.length)
        );

        try (
            Socket socket = new Socket("localhost", 9092);
            DataOutputStream output = new DataOutputStream(
                socket.getOutputStream()
            );
            DataInputStream input = new DataInputStream(
                socket.getInputStream()
            )
        ) {

            output.writeUTF("PRODUCE");
            output.writeUTF(topic);
            output.writeInt(partition);
            output.writeUTF(message);
            output.flush();

            System.out.println(input.readUTF());
        }
    }
}
