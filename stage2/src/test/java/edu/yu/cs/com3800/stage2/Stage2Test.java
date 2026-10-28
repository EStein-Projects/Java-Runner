package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class Stage2Test {

    private static int port = 8000;

    @BeforeEach
    void setUp() {
    }


    @Test
    void electionTests() throws InterruptedException {
        tradeMessages(5L); // Odd
        tradeMessages(8L); // Even
    }

    static void tradeMessages(long max) {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        for (int i = 1; i <= max; i++, port+=10) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", port));
        }
        //create servers
        ArrayList<PeerServerImpl> servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            @SuppressWarnings("unchecked")
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
            servers.add(server);
            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
        }
        //wait for threads to start
        try {
            Thread.sleep(5000);
        }
        catch (Exception ignored) {
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
                Thread.currentThread().interrupt();
            }
        }
    }
}