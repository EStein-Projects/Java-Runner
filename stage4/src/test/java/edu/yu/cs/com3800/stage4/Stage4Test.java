package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static edu.yu.cs.com3800.Message.MessageType.WORK;
import static org.junit.jupiter.api.Assertions.*;

class Stage4Test {

    private static final int startPort = 8000;
    public static final String LOCALHOST = "localhost";
    private static final int myPort = 9999;
    private static final int gatekeeperPort = 8888;
    private static final String HELLO_WORLD_CLASS = "public class HelloWorld {" +
            "public HelloWorld(){}" +
            "public String run() {" +
                "return \"Hello world\";" +
            "}" +
    "}";

    private static final String SCHEME = "HTTP".toLowerCase();
    private static HttpClient httpClient;
    private static final long PEER_COUNT = 4L;
    private static final long GATEWAY_ID = PEER_COUNT + 1L;
    private static final int GATEWAY_UDP_PORT = startPort + 40;
    private static final int NUM_OBSERVERS = 1;

    @Test
    void wrongPathClient() throws URISyntaxException, IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(
                SCHEME, null, "localhost", gatekeeperPort,
                "/", null, null
        ));
        HttpResponse<String> response = httpClient.send(
                builder.setHeader("Content-Type", "text/x-java-source")
                        .PUT(HttpRequest.BodyPublishers.ofString(HELLO_WORLD_CLASS))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );

        int expectedCode = 404;
        int actualCode = response.statusCode();
        System.out.println("\nWrong path client:\n");
        assertEquals(expectedCode, actualCode);
    }

    @Test
    void failedPutRequestClient() throws URISyntaxException, IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(
                SCHEME, null, "localhost", gatekeeperPort,
                "/compileandrun", null, null
        ));
        HttpResponse<String> response = httpClient.send(
                builder.setHeader("Content-Type", "text/x-java-source")
                        .PUT(HttpRequest.BodyPublishers.ofString(HELLO_WORLD_CLASS))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );

        int expectedCode = 405;
        int actualCode = response.statusCode();
        System.out.println("\nFailed put request client:\n");
        assertEquals(expectedCode, actualCode);
    }

    @Test
    void badHeaderClient() throws URISyntaxException, IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(
                SCHEME, null, "localhost", gatekeeperPort,
                "/compileandrun", null, null
        ));
        HttpResponse<String> response = httpClient.send(
                builder.setHeader("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(HELLO_WORLD_CLASS))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );

        int expectedCode = 400;
        int actualCode = response.statusCode();
        System.out.println("\nBad header client:\n");
        assertEquals(expectedCode, actualCode);
    }

    @BeforeEach
    void setUp() {
        try {
            Thread.sleep(10000); // Wait for threads to completely shut down
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Pretest sleep complete");
        httpClient = HttpClient.newHttpClient();
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
            String expected = new JavaRunner().compileAndRun(
                    new ByteArrayInputStream(HELLO_WORLD_CLASS.getBytes())
            );
            runHttpTest(HELLO_WORLD_CLASS, expected, 6);
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
        runHttpTest(unrunnableClass, expectedResult, 4);
    }


    private static void runHttpTest(String baseCode, String expected, int totalRequests) {
        List<PeerServerImpl> servers = startServers(PEER_COUNT, GATEWAY_ID, GATEWAY_UDP_PORT, NUM_OBSERVERS);
        GatewayServer gatewayServer;
        try {
            Map<Long, InetSocketAddress> peerIDtoAddress = new LinkedHashMap<>();
            int port = startPort + 10;
            for (long i = 1; i <= PEER_COUNT; i++, port += 10) {
                peerIDtoAddress.put(i, new InetSocketAddress(LOCALHOST, port));
            }
            peerIDtoAddress.put(GATEWAY_ID, new InetSocketAddress(LOCALHOST, GATEWAY_UDP_PORT));
            ConcurrentHashMap<Long, InetSocketAddress> map = new ConcurrentHashMap<>(peerIDtoAddress);
            gatewayServer = new GatewayServer(gatekeeperPort, GATEWAY_UDP_PORT, 0L, GATEWAY_ID, map, NUM_OBSERVERS);
            gatewayServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            Thread.sleep(10000);
            String clientURL = SCHEME + "://localhost:" + gatekeeperPort + "/compileandrun";
            StringBuilder completeResponse = new StringBuilder();
            for (int i = 1; i <= totalRequests; i++) {
                String requestBase;
                String expectedRequestIndex;
                if (i % 2 == 1) {
                    requestBase = baseCode.replace(" world", " world from request " + i);
                    expectedRequestIndex = String.valueOf(i);
                } else {
                    requestBase = baseCode.replace(" world", " world from request " + (i - 1));
                    expectedRequestIndex = String.valueOf(i - 1);
                }
                HttpRequest request = buildHttpRequest(clientURL, requestBase);
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                boolean expectedCached = (i % 2 == 0);
                String expectedCacheHeader = String.valueOf(expectedCached);
                String actualCacheHeader = response.headers().firstValue("Cached-Response").orElse("Header Missing");
                assertEquals(expectedCacheHeader, actualCacheHeader, "Request " + i + " failed cache assertion.");
                int expectedCode = (baseCode.equals(HELLO_WORLD_CLASS)) ? 200 : 400;
                assertEquals(expectedCode, response.statusCode());
                String modExpected = expected.replace(" world", " world from request " + expectedRequestIndex);
                assertEquals(getTrace(modExpected), getTrace(response.body()), "Content mismatch for request " + i);
                completeResponse
                        .append("Request ").append(i)
                        .append(" (Based on ID: ").append(expectedRequestIndex).append(")")
                        .append("\nStatus: ").append(response.statusCode())
                        .append(", Cached: ").append(actualCacheHeader)
                        .append("\nResponse Body (first part):\n")
                        .append(getTrace(response.body()))
                        .append("\n\n");
            }
            System.out.println(completeResponse);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            for (PeerServerImpl server : servers) {
                server.shutdown();
                try {
                    server.join();
                } catch (InterruptedException ignored) {}
            }
            if (gatewayServer != null) {
                gatewayServer.shutdown();
                try {
                    gatewayServer.join();
                } catch (InterruptedException ignored) {}
            }
        }
    }

    private static HttpRequest buildHttpRequest(String uri, String body) throws URISyntaxException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(uri));
        return builder.setHeader("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private static List<PeerServerImpl> startServers(long maxPeers, long gatewayId, int gatewayUdpPort, int numberOfObservers) {

        Map<Long, InetSocketAddress> peerIDtoAddress = new LinkedHashMap<>();
        int port = startPort;

        for (long i = 1; i <= maxPeers; i++, port += 10) {
            peerIDtoAddress.put(i, new InetSocketAddress(LOCALHOST, port));
        }
        peerIDtoAddress.put(gatewayId, new InetSocketAddress(LOCALHOST, gatewayUdpPort));

        List<PeerServerImpl> servers = new ArrayList<>();

        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getKey().equals(gatewayId)) {
                continue;
            }

            PeerServerImpl server;
            try {
                server = new PeerServerImpl(entry.getValue().getPort(), 0L, entry.getKey(), peerIDtoAddress, gatewayId, numberOfObservers);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            servers.add(server);
            server.start();
        }

        return servers;
    }

    private static String getTrace(String entireBody) {
        int i = entireBody.indexOf("70)");
        if (i == -1) {
            return entireBody;
        }
        return entireBody.substring(0, i+3);
    }


    private static ArrayList<PeerServerImpl> startServers(long max) {
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new LinkedHashMap<>();
        int port = startPort;
        for (int i = 1; i <= max; i++, port += 10) {
            peerIDtoAddress.put((long) i, new InetSocketAddress(LOCALHOST, port));
        }
        ArrayList<PeerServerImpl> servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            @SuppressWarnings("unchecked")
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            PeerServerImpl server;
            try {
                server = new PeerServerImpl(entry.getValue().getPort(), 0L, entry.getKey(), map, 0L, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            servers.add(server);
            server.start();
        }
        return servers;
    }
}