package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.COMPLETED_WORK;
import static edu.yu.cs.com3800.Message.MessageType.WORK;
import static edu.yu.cs.com3800.PeerServer.ServerState.LEADING;

public class RoundRobinLeader implements Runnable, LoggingServer {
    private final PeerServer server;
    private final String myHost;
    private final int myPort;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final List<InetSocketAddress> peerAddresses;
    private Logger logger;

    private long curReqID;
    private final Map<Long, InetSocketAddress> clientMap;

    public RoundRobinLeader(
            PeerServer server, LinkedBlockingQueue<Message> outgoingMessages, LinkedBlockingQueue<Message> incomingMessages,
            Map<Long, InetSocketAddress> peerIDtoAddress
    ) {
        this.server = server;
        this.myHost = server.getAddress().getHostString();
        this.myPort = server.getUdpPort();
        this.outgoingMessages = outgoingMessages;
        this.incomingMessages = incomingMessages;
        this.peerAddresses = new ArrayList<>(peerIDtoAddress.values());
        try {
            this.logger = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + this.myPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.curReqID = 0;
        this.clientMap = new HashMap<>();
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.fine("Thread started");
        while (server.getPeerState() == LEADING && !Thread.currentThread().isInterrupted()) {
            try {
                Message m = incomingMessages.take();
                if (m.getMessageType() == WORK) {
                    InetSocketAddress peer = peerAddresses.get(Math.toIntExact(curReqID++ % peerAddresses.size()));
                    logger.fine("Received request #" + curReqID);
                    outgoingMessages.put(new Message(
                            WORK, m.getMessageContents(), myHost, myPort, peer.getHostString(), peer.getPort(), curReqID
                    ));
                    clientMap.put(curReqID, new InetSocketAddress(m.getSenderHost(), m.getSenderPort()));
                    logger.fine("Assigned request #" + curReqID + " to follower with port: " + peer.getPort());
                } else if (m.getMessageType() == COMPLETED_WORK) {
                    logger.fine("Received completed request #" + m.getRequestID() + " from follower with port: " + m.getSenderPort());
                    InetSocketAddress client = clientMap.remove(m.getRequestID());
                    outgoingMessages.put(new Message(
                            COMPLETED_WORK, m.getMessageContents(), myHost, myPort, client.getHostString(), client.getPort(), m.getRequestID()
                    ));
                    logger.fine("Returned request #" + m.getRequestID() + " to client.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
