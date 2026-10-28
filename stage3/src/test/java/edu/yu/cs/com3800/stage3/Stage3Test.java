package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static edu.yu.cs.com3800.Message.MessageType.WORK;
import static edu.yu.cs.com3800.Message.MessageType.getType;
import static org.junit.jupiter.api.Assertions.*;

class Stage3Test {

    private static final int startPort = 8000;
    public static final String LOCALHOST = "localhost";
    private static final int myPort = 9999;
    private static final String HELLO_WORLD_CLASS = "public class HelloWorld {" +
            "public HelloWorld(){}" +
            "public String run() {" +
                "return \"Hello world\";" +
            "}" +
    "}";

    @BeforeEach
    void setUp() {
        try {
            Thread.sleep(10000); // Wait for threads to completely shut down
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Pretest sleep complete");
    }


    @Test
    void electionTests() {
        runElection(7L); // Odd
        try {
            Thread.sleep(10000); // Wait for threads to completely shut down
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        runElection(8L); // Even
    }

    static void runElection(long max) {
        ArrayList<PeerServerImpl> servers = startServers(max);
        try {
            Thread.sleep(5000); // Wait for threads to start
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (PeerServerImpl server : servers) {
            Vote curLeader = server.getCurrentLeader();
            server.shutdown();
            assertNotNull(curLeader);
            assertEquals(max, curLeader.getProposedLeaderID());
            if (server.getServerId() == max) {
                assertEquals(PeerServer.ServerState.LEADING, server.getPeerState());
            } else {
                assertEquals(PeerServer.ServerState.FOLLOWING, server.getPeerState());
            }
            try {
                server.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Runnable input
     */
    @Test
    void leaderFollowersTest() {
        try {
            runUDPExecutorService(HELLO_WORLD_CLASS, new JavaRunner().compileAndRun(
                    new ByteArrayInputStream(HELLO_WORLD_CLASS.getBytes())
            ));
        } catch (IOException | ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Unrunnable input
     */
    @Test
    void badJavaRunnerTest() {
        String unrunnableClass = HELLO_WORLD_CLASS.replace("run()", "print()");
        Exception e = assertThrows(Exception.class, () ->
                new JavaRunner().compileAndRun(
                        new ByteArrayInputStream(unrunnableClass.getBytes())
                )
        );
        var baos = new ByteArrayOutputStream();
        var ps = new PrintStream(baos);
        e.printStackTrace(ps);
        ps.close();
        String expectedResult = e.getMessage() + "\n" + baos;
        runUDPExecutorService(unrunnableClass, expectedResult);
    }

    static void runUDPExecutorService(String baseCode, String expected) {
        LinkedBlockingQueue<Message> outgoingMessages = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
        UDPMessageSender out = new UDPMessageSender(outgoingMessages, myPort);
        UDPMessageReceiver in;
        try {
            in = new UDPMessageReceiver(
                    incomingMessages, new InetSocketAddress(LOCALHOST, myPort), myPort, null
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int max = 4;
        ArrayList<PeerServerImpl> servers = startServers(max);
        try {
            Thread.sleep(5000); // Wait for threads to start
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        int totalRequests = 6;
        for (int i = 1; i <= totalRequests; i++) {
            String code = baseCode.replace(" world", " world from request " + i);
            Message msg = new Message(
                    WORK, code.getBytes(), LOCALHOST, myPort, LOCALHOST, startPort + (max-1)*10
            );
            try {
                outgoingMessages.put(msg);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Util.startAsDaemon(in, "Receiver");
        Util.startAsDaemon(out, "Sender");
        StringBuilder completeResponse = new StringBuilder();
        for (int i = 1; i <= totalRequests; i++) {
            Message msg;
            try {
                msg = incomingMessages.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            String response = new String(msg.getMessageContents());
            String modExpected = expected.replace(" world", " world from request " + msg.getRequestID());
            assertEquals(getTrace(modExpected), getTrace(response));
            completeResponse
                    .append("Response to request ")
                    .append(msg.getRequestID())
                    .append(" from leader using port ")
                    .append(msg.getSenderPort())
                    .append(":\n")
                    .append(response)
                    .append("\n\n");
        }
        System.out.println(completeResponse);
        for (PeerServerImpl server : servers) {
            server.shutdown();
            try {
                server.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        out.shutdown();
        in.shutdown();
        try {
            out.join();
            in.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getTrace(String entireBody) {
        int i = entireBody.indexOf("70)");
        if (i == -1) {
            return entireBody;
        }
        return entireBody.substring(0, i+3);
    }


    private static ArrayList<PeerServerImpl> startServers(long max) {
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        int port = startPort;
        for (int i = 1; i <= max; i++, port += 10) {
            peerIDtoAddress.put((long) i, new InetSocketAddress(LOCALHOST, port));
        }
        ArrayList<PeerServerImpl> servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            @SuppressWarnings("unchecked")
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0L, entry.getKey(), map);
            servers.add(server);
            server.start();
        }
        return servers;
    }
}