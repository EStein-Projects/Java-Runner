package edu.yu.cs.com3800.stage4;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Stage4ProfTest {

    private static final int START_PORT = 8000;
    public static final String LOCALHOST = "localhost";
    private static final long PEER_COUNT = 3L;
    private static final long GATEWAY_ID = PEER_COUNT + 1L;
    private static final int GATEWAY_UDP_PORT = Math.toIntExact(START_PORT + PEER_COUNT * 10);
    private static final int GATEWAY_HTTP_PORT = 8888;
    private static final int NUM_OBSERVERS = 1;
    private static final String SCHEME = "HTTP".toLowerCase();
    private static final String HELLO_WORLD_CLASS = "public class HelloWorld {" +
        "public HelloWorld(){}" +
        "public String run() {" +
            "return \"Hello world\";" +
        "}" +
    "}";

    @Test
    void test() throws InterruptedException, IOException, URISyntaxException {
        GatewayServer gatewayServer = getGatewayServer();
        List<PeerServerImpl> servers = startServers();
        Thread.sleep(25000);
        sendMessage(HELLO_WORLD_CLASS, gatewayServer);
        try (ExecutorService executor = Executors.newFixedThreadPool((int) PEER_COUNT)) {
            for (int i = 0; i < PEER_COUNT; i++) {
                final int index = i;
                executor.submit(() -> {
                    String code = HELLO_WORLD_CLASS.replace(
                            " world", " world from code version " + index
                    );
                    try {
                        sendMessage(code, gatewayServer);
                    } catch (Exception ignored) {}
                });
            }
        }
        for (PeerServerImpl s: servers) {
            s.shutdown();
        }
        sendMessage(HELLO_WORLD_CLASS, gatewayServer);
        gatewayServer.shutdown();
        for (PeerServerImpl s: servers) {
            s.join();
        }
        gatewayServer.join();
    }

    private GatewayServer getGatewayServer() throws IOException {
        Map<Long, InetSocketAddress> peerIDtoAddress = new LinkedHashMap<>();
        int port = START_PORT;
        for (long i = 1; i <= PEER_COUNT; i++, port += 10) {
            peerIDtoAddress.put(i, new InetSocketAddress(LOCALHOST, port));
        }
        peerIDtoAddress.put(GATEWAY_ID, new InetSocketAddress(LOCALHOST, GATEWAY_UDP_PORT));
        ConcurrentHashMap<Long, InetSocketAddress> map = new ConcurrentHashMap<>(peerIDtoAddress);
        GatewayServer gatewayServer = new GatewayServer(GATEWAY_HTTP_PORT, GATEWAY_UDP_PORT, 0L, GATEWAY_ID, map, NUM_OBSERVERS);
        gatewayServer.start();
        return gatewayServer;
    }

    private void sendMessage(String code, GatewayServer servers) throws URISyntaxException, IOException, InterruptedException {
        String clientURL = SCHEME + "://localhost:" + GATEWAY_HTTP_PORT + "/compileandrun";
        HttpRequest request = buildHttpRequest(clientURL, code);
        HttpResponse<String> response;
        try (HttpClient client = HttpClient.newHttpClient()) {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("request sent");
        }
        System.out.println(response.statusCode());
        System.out.println(response.body());
    }

    private static List<PeerServerImpl> startServers() throws IOException {
        Map<Long, InetSocketAddress> peerIDtoAddress = new LinkedHashMap<>();
        int port = START_PORT;
        for (long i = 1; i <= PEER_COUNT; i++, port += 10) {
            peerIDtoAddress.put(i, new InetSocketAddress(LOCALHOST, port));
        }
        peerIDtoAddress.put(GATEWAY_ID, new InetSocketAddress(LOCALHOST, GATEWAY_UDP_PORT));
        List<PeerServerImpl> servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getKey().equals(GATEWAY_ID)) {
                continue;
            }
            PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0L, entry.getKey(), peerIDtoAddress, GATEWAY_ID, NUM_OBSERVERS);
            servers.add(server);
            server.start();
        }
        return servers;
    }

    private static HttpRequest buildHttpRequest(String uri, String body) throws URISyntaxException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(uri));
        return builder.setHeader("Content-Type", "text/x-java-source")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }
}
