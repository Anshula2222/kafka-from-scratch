package org.example;
import org.example.broker.BrokerServer;

public class App {

    public static void main(String[] args) throws Exception {

        BrokerServer broker = new BrokerServer(9092);

        broker.start();
    }
}
