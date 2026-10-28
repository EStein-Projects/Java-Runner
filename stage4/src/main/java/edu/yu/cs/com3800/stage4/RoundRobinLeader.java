package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.COMPLETED_WORK;
import static edu.yu.cs.com3800.Message.MessageType.WORK;
import static edu.yu.cs.com3800.PeerServer.ServerState.LEADING;

public class RoundRobinLeader implements Runnable, LoggingServer {
    private final PeerServer server;
    private final String myHost;
    private final int myTCPPort;
    private final List<InetSocketAddress> peerAddresses;
    private final Logger logger;

    private long curReqID;

    private static final int TIMEOUT = 2000;

    public RoundRobinLeader(
            PeerServer server, Map<Long, InetSocketAddress> peerIDtoAddress
    ) throws IOException {
        this.server = server;
        this.myHost = server.getAddress().getHostString();
        this.myTCPPort = server.getUdpPort() + 2;
        this.peerAddresses = getPeersList(peerIDtoAddress);
        this.curReqID = 0;
        this.logger = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + this.myTCPPort);
        this.logger.fine("RoundRobinLeader instance created");
    }

    private List<InetSocketAddress> getPeersList(Map<Long, InetSocketAddress> peerIDtoAddress) {
        if (peerIDtoAddress instanceof SequencedMap<Long, InetSocketAddress>) {
            return new ArrayList<>(peerIDtoAddress.values());
        } else {
            return new ArrayList<>(new TreeMap<>(peerIDtoAddress).values());
        }
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.fine("Thread started");
        List<InetSocketAddress> followers = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(peerAddresses.size());
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (InetSocketAddress udpAddress : peerAddresses) {
                executor.execute(() -> {
                    InetSocketAddress tcpAddress = new InetSocketAddress(
                            udpAddress.getHostString(),
                            udpAddress.getPort() + 2
                    );
                    try (Socket peer = new Socket()) {
                        peer.connect(tcpAddress, TIMEOUT);
                        followers.add(tcpAddress);
                    } catch (ConnectException | SocketTimeoutException e) {
                        logger.finer(
                                "Dropped peer with address " + tcpAddress
                                        + " from followers list due to " + e
                        );
                    } catch (IOException e) {
                        logger.warning("IOException thrown when initially connecting to " + tcpAddress);
                        logger.finer(
                                "Dropped peer with address " + tcpAddress
                                        + " from followers list due to " + e
                        );
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.fine("Thread stopped");
            return;
        }
        try (
                ServerSocket gatewayReceiver = new ServerSocket(myTCPPort);
                ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()
        ) {
            while (server.getPeerState() == LEADING && !Thread.currentThread().isInterrupted()) {
                final InetSocketAddress peer = followers.get(Math.toIntExact(curReqID++ % followers.size()));
                final Message request;
                final Socket gatewayConnection = gatewayReceiver.accept();
                InputStream gis = gatewayConnection.getInputStream();
                Message rawRequest = new Message(Util.readAllBytesFromNetwork(gis));
                logger.fine("Received request #" + curReqID + " from gateway");
                request = new Message(
                        WORK, rawRequest.getMessageContents(), myHost, myTCPPort,
                        peer.getHostString(), peer.getPort(), curReqID
                );
                executor.execute(getRunnable(gatewayConnection, peer, request, curReqID));
                logger.fine("Assigning request #" + curReqID + " to follower with tcp port: " + peer.getPort());
            }
        } catch (IOException e) {
            logger.warning("IOException thrown when processing a request");
        }
        logger.fine("Thread stopped");
    }

    private Runnable getRunnable (Socket gatewayResponder, final InetSocketAddress peer, final Message request, final long reqID) {
        return () -> {
            try {
                Message response;
                try (Socket follower = new Socket()) {
                    follower.connect(peer);
                    OutputStream fos = follower.getOutputStream();
                    fos.write(request.getNetworkPayload());
                    fos.flush();
                    logger.finer("Sent request #" + reqID + " to follower with tcp port: " + peer.getPort());
                    InputStream fis = follower.getInputStream();
                    response = new Message(Util.readAllBytesFromNetwork(fis));
                    }
                logger.finer("Received completed request #" + request.getRequestID() + " from follower with port: " + response.getSenderPort());
                try (OutputStream gos = gatewayResponder.getOutputStream()) {
                    gos.write(new Message(
                            COMPLETED_WORK, response.getMessageContents(), myHost, myTCPPort,
                            gatewayResponder.getInetAddress().getHostName(), gatewayResponder.getPort(),
                            reqID, response.getErrorOccurred()
                    ).getNetworkPayload());
                    gos.flush();
                }
                logger.finer("Returned request #" + response.getRequestID() + " to gatekeeper");
            } catch (IOException e) {
                logger.warning("IOException thrown in a responding virtual thread");
            }
        };
    }
}