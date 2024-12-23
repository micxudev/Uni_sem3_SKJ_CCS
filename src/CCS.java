import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CCS {
    static int port;
    static DatagramSocket socket_UDP;
    static ServerSocket server_TCP;
    static final String DISCOVERY_MSG = "CCS DISCOVER";
    static final byte[] CCS_FOUND_BYTES = "CCS FOUND".getBytes(StandardCharsets.UTF_8);
    static final String ERROR = "ERROR";
    static final HashSet<String> opers = new HashSet<>(Arrays.asList("ADD", "SUB", "MUL", "DIV"));
    static final Stats stats = new Stats();
    static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) {
        checkProgramArgs(args);
        try {
            // UDP
            socket_UDP = new DatagramSocket(port);
            port = socket_UDP.getLocalPort(); // in case of port == 0, OS gives random one, so this line saves the actual port in that case

            // TCP
            server_TCP = new ServerSocket(port);

            // run both
            runUDP();
            runTCP();

            // schedule statistics executor
            executor.scheduleAtFixedRate(() -> {
                System.out.println(stats.getStats());
                stats.resetLast10SecStats();
            }, 10, 10, TimeUnit.SECONDS);

        } catch (IOException e) {
            System.err.println("Could not start UDP/TCP on port: " + port);
            System.exit(1);
        }
    }

    static void checkProgramArgs(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar CCS.jar <port>");
            System.exit(1);
        }

        try {
            port = Integer.parseInt(args[0]);
            if (port < 0 || port > 0xFFFF) {
                throw new IllegalArgumentException();
            }
        } catch (NumberFormatException e) {
            System.err.println("Port is not an integer: " + args[0]);
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.err.println("Port is out of range: " + args[0]);
            System.exit(1);
        }
    }

    static void runUDP() {
        new Thread(() -> {
            System.out.println(Thread.currentThread().getName() + "Started, listening on port: " + port);
            try {
                DatagramPacket packet = new DatagramPacket(new byte[DISCOVERY_MSG.length()], DISCOVERY_MSG.length());
                while (true) {
                    socket_UDP.receive(packet);

                    String message = new String(packet.getData());
                    if (message.equals(DISCOVERY_MSG)) {
                        DatagramPacket response = new DatagramPacket(
                            CCS_FOUND_BYTES, 0, CCS_FOUND_BYTES.length,
                            packet.getSocketAddress()
                        );
                        socket_UDP.send(response);
                    }
                }
            } catch (IOException e) {
                System.err.println(Thread.currentThread().getName() + "Error: " + e.getMessage());
            } finally {
                System.out.println(Thread.currentThread().getName() + "Stopped running");
            }
        }, "[Thread/UDP] ").start();
    }

    static void runTCP() {
        new Thread(() -> {
            System.out.println(Thread.currentThread().getName() + "Started, listening on port: " + port);
            try {
                while (true) {
                    Socket socket = server_TCP.accept();
                    stats.incClients();
                    new Thread(() -> {
                        try {
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);

                            String line;
                            while ((line = in.readLine()) != null) {
                                stats.incRequests();
                                String result = parseTCPRequestLine(line);
                                out.println(result);
                                System.out.println(Thread.currentThread().getName() +
                                    "Request: '" + line + "'. Result: '" + result + "'");
                            }
                        } catch (IOException e) {
                            System.err.println(Thread.currentThread().getName() + "Exception: " + e.getMessage());
                        }
                    }, "[Thread/CH_TCP] ").start();
                }
            } catch (IOException e) {
                System.err.println(Thread.currentThread().getName() + "Error: " + e.getMessage());
            } finally {
                System.out.println(Thread.currentThread().getName() + "Stopped running");
            }
        }, "[Thread/TCP] ").start();
    }

    static String parseTCPRequestLine(String line) {
        String[] parts = line.trim().split(" ");

        if (parts.length != 3) {
            stats.incIncorOps();
            return ERROR;
        }

        String oper = parts[0];
        if (!opers.contains(oper)) {
            stats.incIncorOps();
            return ERROR;
        }

        Integer int_arg1 = parseInteger(parts[1]);
        Integer int_arg2 = parseInteger(parts[2]);
        if (int_arg1 == null || int_arg2 == null) {
            stats.incIncorOps();
            return ERROR;
        }

        String res;
        switch (oper) {
            case "ADD":
                res = String.valueOf(int_arg1 + int_arg2);
                stats.incOper("ADD");
                stats.addSum(int_arg1 + int_arg2);
                break;
            case "SUB":
                res = String.valueOf(int_arg1 - int_arg2);
                stats.incOper("SUB");
                stats.addSum(int_arg1 - int_arg2);
                break;
            case "MUL":
                res = String.valueOf(int_arg1 * int_arg2);
                stats.incOper("MUL");
                stats.addSum(int_arg1 * int_arg2);
                break;
            case "DIV":
                if (int_arg2 == 0) {
                    stats.incIncorOps();
                    return ERROR;
                }
                res = String.valueOf(int_arg1 / int_arg2);
                stats.incOper("DIV");
                stats.addSum(int_arg1 / int_arg2);
                break;
            default:
                stats.incIncorOps();
                res = ERROR;
                break;
        }

        return res;
    }

    static Integer parseInteger(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static class Stats {
        private int totalClients = 0;
        private int totalRequests = 0;
        private final HashMap<String, Integer> totalOps = new HashMap<>();
        private int totalIncorOps = 0;
        private int totalSum = 0;

        private int clientsLast10Sec = 0;
        private int requestsLast10Sec = 0;
        private final HashMap<String, Integer> opsLast10Sec = new HashMap<>();
        private int incorOpsLast10Sec = 0;
        private int sumLast10Sec = 0;

        public synchronized void incClients() {
            totalClients++;
            clientsLast10Sec++;
        }

        public synchronized void incRequests() {
            totalRequests++;
            requestsLast10Sec++;
        }

        public synchronized void incOper(String op) {
            totalOps.put(op, totalOps.getOrDefault(op, 0) + 1);
            opsLast10Sec.put(op, opsLast10Sec.getOrDefault(op, 0) + 1);
        }

        public synchronized void incIncorOps() {
            totalIncorOps++;
            incorOpsLast10Sec++;
        }

        public synchronized void addSum(int value) {
            totalSum += value;
            sumLast10Sec += value;
        }

        public synchronized void resetLast10SecStats() {
            clientsLast10Sec = 0;
            requestsLast10Sec = 0;
            opsLast10Sec.clear();
            incorOpsLast10Sec = 0;
            sumLast10Sec = 0;
        }

        // getStats() might show some errors before first run (but should compile correctly)
        public synchronized String getStats() {
            return String.format(
                "%n--------------- Statistics ---------------%n" +
                "Total:%n" +
                " · Clients: %d%n" +
                " · Requests: %d%n" +
                " · Incorrect Ops: %d%n" +
                " · Sum: %d%n" +
                " · Ops: %s%n" +
                "Last 10 secs:%n" +
                " · Clients: %d%n" +
                " · Requests: %d%n" +
                " · Incorrect Ops: %d%n" +
                " · Sum: %d%n" +
                " · Ops: %s%n" +
                "------------------------------------------%n",
                totalClients, totalRequests, totalIncorOps, totalSum, totalOps,
                clientsLast10Sec, requestsLast10Sec, incorOpsLast10Sec, sumLast10Sec, opsLast10Sec
            );
        }
    }
}