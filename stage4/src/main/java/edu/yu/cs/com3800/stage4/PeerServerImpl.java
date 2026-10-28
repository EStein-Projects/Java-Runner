package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.ELECTION;
import static edu.yu.cs.com3800.PeerServer.ServerState.*;


public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {
    private final InetSocketAddress myAddress;
    private final int myUDPPort;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private final Map<Long, InetSocketAddress> peerAddressMap;
//    private final InetSocketAddress gateway;
    private final int quorum;
    private final Logger logger;

    private volatile Vote currentLeader;
    private ServerState state;
    private long peerEpoch;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private Thread leaderOrFollower;

    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress,
                          Long gatewayID, int numberOfObservers) throws IOException {
        this.id = serverID;
        if (peerIDtoAddress.containsKey(this.id)) {
            this.myAddress = peerIDtoAddress.get(this.id);
            this.peerAddressMap = new TreeMap<>();
            for (Map.Entry<Long, InetSocketAddress> e: peerIDtoAddress.entrySet()) {
                if (!e.getKey().equals(this.id)) {
                    this.peerAddressMap.put(e.getKey(), e.getValue());
                }
            }
        } else {
            this.myAddress = new InetSocketAddress("localhost", udpPort);
            this.peerAddressMap = peerIDtoAddress;
        }
//        this.gateway = this.peerAddressMap.get(gatewayID);
//        if (this.gateway == null && !this.id.equals(gatewayID)) {
//            throw new IllegalArgumentException("Gateway is mising from peer address map");
//        }
        this.myUDPPort = udpPort;
        this.state = LOOKING;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.peerEpoch = peerEpoch;
        this.currentLeader = null;
        this.quorum = (this.peerAddressMap.size() + 1 - numberOfObservers) /2 + 1;
        this.logger = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + this.myUDPPort);
        setName("PeerServer-ID-" + this.id + "-Port-" + this.myUDPPort);
        this.logger.fine("PeerServerImpl instance created");
    }

    @Override
    public void shutdown() {
        interrupt();
        if (senderWorker != null) {
            senderWorker.shutdown();
        }
        if (leaderOrFollower != null) {
            leaderOrFollower.interrupt();
        }
        if (receiverWorker != null) {
            receiverWorker.shutdown();
        }
    }

    @Override
    public synchronized void setCurrentLeader(Vote v) throws IOException {
        if (isInterrupted()) {
            return;
        }
        currentLeader = v;
        if (state == OBSERVER) {
            currentLeader = v;
        }
        if (v.getProposedLeaderID() == id) {
            state = LEADING;
        } else {
            state = FOLLOWING;
        }
//        // This should probably be handled by a separate failure method
//        if (v == null) {
//            state = LOOKING;
//        }
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        if (isInterrupted()) {
            return;
        }
        Message msg = new Message(ELECTION, messageContents, this.myAddress.getHostString(), this.myUDPPort,
                target.getHostString(), target.getPort());
        try {
            outgoingMessages.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        if (isInterrupted()) {
            return;
        }
        for(InetSocketAddress peer : peerAddressMap.values()) {
            sendMessage(type, messageContents, peer);
        }
    }

    @Override
    public ServerState getPeerState() {
        return state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        if (!isInterrupted()) {
            if (newState == null) {
                throw new NullPointerException();
            }
            // In the future if desired to make servers actually modifiable to OBSERVERs,
            // Should possibly add quorum modifying logic here
            state = newState;
        }
    }

    @Override
    public Long getServerId() {
        return id;
    }

    @Override
    public long getPeerEpoch() {
        return peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return myAddress;
    }

    @Override
    public int getUdpPort() {
        return myUDPPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerAddressMap.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return quorum;
    }

    @Override
    public void run() {
        if (isInterrupted()) {
            return;
        }
        logger.fine("Thread started");
        senderWorker = new UDPMessageSender(this.outgoingMessages, this.myUDPPort);
        senderWorker.start();
        try{
            receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myUDPPort, this);
        } catch(IOException e){
            e.printStackTrace();
            shutdown();
            return;
        }
        receiverWorker.start();

        LeaderElection election = new LeaderElection(this, incomingMessages, logger);
        try{
            while (!isInterrupted()){
                switch (state) {
                    case LOOKING, OBSERVER -> runElection(election);
                    case FOLLOWING -> leaderOrFollower = new Thread(
                            new JavaRunnerFollower(this),
                            "JavaRunnerFollower-ID-" + id + "-Port-" + myUDPPort
                    );
                    case LEADING -> leaderOrFollower = new Thread(
                            new RoundRobinLeader(this, peerAddressMap),
                            "RoundRobinLeader-ID-" + id + "-Port-" + myUDPPort);
//                    // Not sure about this
//                    case OBSERVER -> {
//                        if (currentLeader == null) {
//                            runElection(election);
//                        }
//                    }
                }
                if (leaderOrFollower != null) {
                    leaderOrFollower.setDaemon(true);
                    leaderOrFollower.start();
                    try {
                        leaderOrFollower.join();
                    } catch (InterruptedException e) {
                        interrupt();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutdown();
        logger.fine("Thread stopped");
    }

    private void runElection(LeaderElection election) throws IOException {
        currentLeader = election.lookForLeader();
        setCurrentLeader(currentLeader);
        logger.fine("Election complete. Winner: " + currentLeader);
    }
}