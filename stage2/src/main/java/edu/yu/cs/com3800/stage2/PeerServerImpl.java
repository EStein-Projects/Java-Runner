package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.ELECTION;


public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutDown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;
    private Logger logger;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    public PeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress) {
        //code here...
        this.myAddress = new InetSocketAddress("localhost",myPort);
        this.myPort = myPort;
        this.state = ServerState.LOOKING;
        this.shutDown = true;
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
        if (this.shutDown) {
            return;
        }
        this.shutDown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        interrupt();
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        if (!shutDown) {
            this.currentLeader = v;
        }
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        if (shutDown) {
            return;
        }
        Message msg = new Message(ELECTION, messageContents, this.myAddress.getHostString(), this.myPort,
                target.getHostString(), target.getPort());
        try {
            this.outgoingMessages.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        if (shutDown) {
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
        if (!shutDown) {
            this.state = newState;
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
        if (!shutDown) {
            return;
        }
        logger.fine("Thread started");
        shutDown = false;
        //step 1: create and run thread that sends broadcast messages
        senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        senderWorker.start();
        //step 2: create and run thread that listens for messages sent to this server
        try{
            receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
        } catch(IOException e){
            e.printStackTrace();
            shutdown();
            return;
        }
        receiverWorker.start();
        //step 3: main server loop
        LeaderElection election = new LeaderElection(this, incomingMessages, logger);
        try{
            while (!shutDown){
                if (state == ServerState.LOOKING) {//start leader election, set leader to the election winner
                    currentLeader = election.lookForLeader();
                    logger.fine("Election complete. Winner: " + currentLeader);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            shutdown();
        }
    }

}
