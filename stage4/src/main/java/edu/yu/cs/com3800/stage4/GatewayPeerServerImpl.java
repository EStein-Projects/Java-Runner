package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Vote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;

import static edu.yu.cs.com3800.PeerServer.ServerState.OBSERVER;

public class GatewayPeerServerImpl extends PeerServerImpl {
    private LeaderSharer sharer;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;

    public GatewayPeerServerImpl(
            int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers
    ) throws IOException {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, gatewayID, numberOfObservers);
        super.setPeerState(OBSERVER);
        this.peerIDtoAddress = peerIDtoAddress;
    }

    public void setSharer(LeaderSharer leaderSharer) {
        if (isInterrupted()) {
            return;
        }
        if (this.sharer != null) {
            throw new IllegalStateException();
        }
        Objects.requireNonNull(leaderSharer);
        this.sharer = leaderSharer;
    }

    /**
     * @throws UnsupportedOperationException a gateway server can only ever be an observer
     */
    @Override public void setPeerState(ServerState newState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void setCurrentLeader(Vote v) throws IOException {
        if (isInterrupted()) {
            return;
        }
        InetSocketAddress leaderUDP = peerIDtoAddress.get(v.getProposedLeaderID());
        try {
            sharer.setLeader(new InetSocketAddress(leaderUDP.getAddress(), leaderUDP.getPort() + 2));
        } catch (InterruptedException e) {
            shutdown();
        }
        super.setCurrentLeader(v);
    }
}