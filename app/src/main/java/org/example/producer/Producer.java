
package org.example.producer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Producer {

    public static void main(String[] args) throws IOException {

        try (
            Socket socket = new Socket("localhost", 9092);
            DataOutputStream output = new DataOutputStream(
                socket.getOutputStream()
            );
            DataInputStream input = new DataInputStream(
                socket.getInputStream()
            )
        ) {

            String message = args.length > 0
                ? String.join(" ", args)
                : "Hello from producer!";

            output.writeUTF("PRODUCE");
            output.writeUTF(message);
            output.flush();

            String response = input.readUTF();

            System.out.println("Broker response: " + response);
        }
    }
}
