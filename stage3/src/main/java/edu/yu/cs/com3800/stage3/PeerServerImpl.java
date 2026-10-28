package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.ELECTION;
import static edu.yu.cs.com3800.PeerServer.ServerState.*;


public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;

    private Logger logger;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private Thread leaderOrFollower;

    public PeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress) {
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.myPort = myPort;
        this.state = LOOKING;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.id = id;
        this.peerEpoch = peerEpoch;
        this.currentLeader = null;
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerIDtoAddress.remove(id);
        try {
            this.logger = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + this.myPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        setName("PeerServer-ID-" + this.id + "-Port-" + this.myPort);
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
    public void setCurrentLeader(Vote v) throws IOException {
        if (!isInterrupted()) {
            if (v == currentLeader) {
                return;
            }
            if (state == LEADING) {
                state = FOLLOWING;
            } else if (v.getProposedLeaderID() == id) {
                state = LEADING;
            }
            currentLeader = v;
        }
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
        Message msg = new Message(ELECTION, messageContents, this.myAddress.getHostString(), this.myPort,
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
        for(InetSocketAddress peer : peerIDtoAddress.values()) {
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
        return myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return peerIDtoAddress.size()/2 + 1;
    }

    @Override
    public void run(){
        if (isInterrupted()) {
            return;
        }
        logger.fine("Thread started");
        senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        senderWorker.start();
        try{
            receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
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
                    case LOOKING -> {
                        currentLeader = election.lookForLeader();
                        logger.fine("Election complete. Winner: " + currentLeader);
                        break;
                    }
                    case FOLLOWING -> {
                        leaderOrFollower = new Thread(new JavaRunnerFollower(
                                this, outgoingMessages, incomingMessages
                        ),"JavaRunnerFollower-ID-" + id + "-Port-" + myPort);
                        break;
                    }
                    case LEADING -> {
                        leaderOrFollower = new Thread(new RoundRobinLeader(
                                this, outgoingMessages, incomingMessages, peerIDtoAddress
                        ),"RoundRobinLeader-ID-" + id + "-Port-" + myPort);
                    }
                }
                if (leaderOrFollower != null) {
                    leaderOrFollower.start();
                    try {
                        leaderOrFollower.join();
                    } catch (InterruptedException e) {
                        interrupt();
                    }
                }
            }
            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            shutdown();
        }
    }
}
