
package org.example.consumer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Consumer {

    public static void main(String[] args) throws IOException {

        long offset = args.length > 0
            ? Long.parseLong(args[0])
            : 0;

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
            output.writeLong(offset);
            output.flush();

            String response = input.readUTF();

            if (response.equals("FOUND")) {
                String message = input.readUTF();

                System.out.println(
                    "Offset " + offset + ": " + message
                );
            } else {
                System.out.println(
                    "Offset " + offset + ": " + response
                );
            }
        }
    }
}
