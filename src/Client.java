import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;

public class Client {
    public static void main(String[] args) throws IOException {
        int port = 6969;

        for (int i = 0; i < 5; i++) {
            // UDP
            DatagramSocket socket_UDP = new DatagramSocket();

            // send UDP
            String discoveryMessage = "CCS DISCOVER";
            byte[] sendData = discoveryMessage.getBytes();

            DatagramPacket send_packet = new DatagramPacket(
                    sendData, sendData.length,
                    InetAddress.getByName("localhost"), port
            );
            socket_UDP.send(send_packet);

            // response UDP
            DatagramPacket receive_packet = new DatagramPacket(new byte[9], 9);
            socket_UDP.receive(receive_packet);

            System.out.println(new String(receive_packet.getData()));

            // address
            InetAddress server_address = receive_packet.getAddress();
            socket_UDP.close();


            // TCP
            Socket socket_TCP = new Socket(server_address, port);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket_TCP.getInputStream()));
            PrintWriter out = new PrintWriter(socket_TCP.getOutputStream(), true);

            out.println("ADD 2 1");
            String add1 = in.readLine();
            System.out.println("ADD result: " + add1); // 3

            out.println("SUB 2 1");
            String sub1 = in.readLine();
            System.out.println("SUB result: " + sub1); // 1

            out.println("MUL 2 1");
            String mul1 = in.readLine();
            System.out.println("MUL result: " + mul1); // 2

            out.println("DIV 20 10");
            String div1 = in.readLine();
            System.out.println("DIV result: " + div1); // 2

            out.println("HI 1000 1000");
            String hi0 = in.readLine();
            System.out.println("HI result: " + hi0); // ERROR

            out.println("DIV 100 0");
            String div100 = in.readLine();
            System.out.println("DIV result: " + div100); // ERROR


            out.println("ADD 100 100 100");
            String add3 = in.readLine();
            System.out.println("ADD result: " + add3); // ERROR

            // Sum: 8. Errors: 3
        }
    }
}