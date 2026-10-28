package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.*;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.WORK;

public class GatewayServer extends Thread implements LoggingServer {
    private final GatewayPeerServerImpl psi;
    private final String myHost;
    private final int myTCPPort;
    private final Logger logger;
    private final LeaderSharer sharer;

    private final ConcurrentHashMap<Integer, byte[]> ranCache;
    private final ConcurrentHashMap<Integer, byte[]> thrownCache;
    private final HttpServer server;

    public GatewayServer(
            int httpPort, int peerPort, long peerEpoch, Long serverID,
            ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers
    ) throws IOException {
        this.psi = new GatewayPeerServerImpl(peerPort, peerEpoch, serverID, peerIDtoAddress, serverID, numberOfObservers);
        this.psi.setName("GatewayPeerServer-ID-" + serverID + "-Port-" + peerPort);
        this.logger = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + httpPort);
        setName("PeerServer-ID-" + serverID + "-Port-" + httpPort);
        this.logger.fine("GatewayServer instance created");
        this.myHost = this.psi.getAddress().getHostString();
        this.myTCPPort = peerPort + 2;
        this.sharer = new LeaderSharer();
        this.psi.setSharer(sharer);

        this.ranCache = new ConcurrentHashMap<>();
        this.thrownCache = new ConcurrentHashMap<>();
        this.server = getHttpServer(httpPort);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    private HttpServer getHttpServer(int httpPort) throws IOException {
        return HttpServer.create(
                new InetSocketAddress(httpPort), 0, "/compileandrun",
                HttpHandlers.handleOrElse(
                        request -> request
                                .getRequestMethod()
                                .equalsIgnoreCase("POST"),
                        HttpHandlers.handleOrElse(
                                request -> request
                                        .getRequestHeaders()
                                        .get("Content-Type")
                                        .contains("text/x-java-source"),
                                getProperHeadersHandler(),
                                HttpHandlers.of(400, Headers.of("Cached-Response", String.valueOf(false)), "")
                        ),
                        HttpHandlers.of(405, Headers.of("Cached-Response", String.valueOf(false)), "")
                ),
                Filter.afterHandler("Log response code",
                        exchange -> logger.finer("Response code sent: " + exchange.getResponseCode())
                )
        );
    }

    private HttpHandler getProperHeadersHandler() {
        return exchange -> {
            int responseCode = 200;
            byte[] responseBody;
            String cached = String.valueOf(false);
            byte[] requestBody = Util.readAllBytes(exchange.getRequestBody());
            int requestHash = Arrays.hashCode(requestBody);
            byte[] cachedResponse = ranCache.get(requestHash);
            if (cachedResponse != null) {
                logger.fine("Cache hit in ran cache for request hash: " + requestHash);
            } else {
                cachedResponse = thrownCache.get(requestHash);
                if (cachedResponse != null) {
                    logger.fine("Cache hit in thrown cache for request hash: " + requestHash);
                    responseCode = 400;
                }
            }
            if (cachedResponse != null) {
                cached = String.valueOf(true);
                responseBody = cachedResponse;
            } else {
                logger.fine("Cache miss for request hash: " + requestHash);
                Message responseMessage = getResponseMessage(requestBody, requestHash);
                if (responseMessage == null) { // Was interrupted
                    return;
                }
                logger.fine("Received response for request with hash " + requestHash);
                responseBody = responseMessage.getMessageContents();
                if (responseMessage.getErrorOccurred()) {
                    responseCode = 400;
                    thrownCache.put(requestHash, responseBody);
                } else {
                    ranCache.put(requestHash, responseBody);
                }
            }
            exchange.getResponseHeaders().set("Cached-Response", cached);
            exchange.sendResponseHeaders(responseCode, responseBody.length);
            /*
            Javadoc on HttpExchange#getResponseBody():
            Closing this stream implicitly closes the InputStream
            returned from getRequestBody() (if it is not already closed).
            */
            try (OutputStream cos = exchange.getResponseBody()) {
                cos.write(responseBody);
                cos.flush();
            }
            logger.fine("Sent response to request with hash " + requestHash);
        };
    }

    private Message getResponseMessage(byte[] requestBody, int requestHash) {
        InetSocketAddress leader;
        try {
            leader = sharer.getLeader();
        } catch (InterruptedException e) {
            return null;
        }
        try (Socket leaderSocket = new Socket()) {
            leaderSocket.connect(leader);
            sendToLeader(leaderSocket, requestBody, leader);
            logger.fine("Sent request with hash " + requestHash + " to leader with address " + leader);
            return getLeaderResponse(leaderSocket);
        } catch (IOException e) {
            // Consider handling this differently:
            // e.g. retries or wait to give chance for a leader update
            // Maybe even set leader to null
            logger.warning("IOException caught: " + e);
            throw new RuntimeException(e);
        }
    }

    private void sendToLeader(Socket leaderSocket, byte[] requestBody, InetSocketAddress leader) throws IOException {
        OutputStream los = leaderSocket.getOutputStream();
        los.write(new Message(
                WORK, requestBody, myHost, myTCPPort,
                leader.getHostString(), leader.getPort()
        ).getNetworkPayload());
        los.flush();
    }

    private Message getLeaderResponse(Socket leaderSocket) throws IOException {
        Message responseMessage;
        InputStream lis = leaderSocket.getInputStream();
        responseMessage = new Message(Util.readAllBytesFromNetwork(lis));
        return responseMessage;
    }

    public GatewayPeerServerImpl getPeerServer() {
        return psi;
    }

    public void shutdown() {
        interrupt();
        server.stop(0);
        psi.shutdown();
    }

    @Override
    public void run() {
        logger.fine("Thread started");
        psi.start();
        logger.fine("PeerServer started");
        server.start();
        logger.fine("HTTPServer started");
        try {
            logger.fine("Starting leader address obtained: " + sharer.getLeader());
            psi.join();
        } catch (InterruptedException ignored) {}
        shutdown();
        logger.fine("Thread stopped");
    }
}