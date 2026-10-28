package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.ELECTION;
import static edu.yu.cs.com3800.PeerServer.ServerState.*;

/**We are implemeting a simplfied version of the election algorithm. For the complete version which covers all possible scenarios, see https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 3200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;

    private final PeerServer server;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Logger logger;
    private long proposedEpoch;
    private long proposedLeader;
    private static final long STARTING_WAIT_TIME = 1875L;

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.logger = logger;
        this.proposedEpoch = server.getPeerEpoch();
        this.proposedLeader = server.getServerId();
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        try {
            //send initial notifications to get things started
            sendNotifications();
            long waitTime = STARTING_WAIT_TIME;
            Map<Long, ElectionNotification> receivedVotes = new HashMap<>();
            //Loop in which we exchange notifications with other servers until we find a leader
            while (server.getPeerState() == LOOKING && !Thread.currentThread().isInterrupted()) {
                //Remove next notification from queue
                Message curMessage = incomingMessages.poll(waitTime, TimeUnit.MILLISECONDS);
                //If no notifications received...
                if (curMessage == null) {
                    //...resend notifications to prompt a reply from others
                    sendNotifications();
                    //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                    if (waitTime < maxNotificationInterval) { // Assuming the max stays at 30,000 milliseconds
                        waitTime = waitTime * 2;
                    }
                //If we did get a message...
                } else {
                    ElectionNotification curEN = getNotificationFromMessage(curMessage);
                    //...if it's for an earlier epoch, or from an observer, ignore it.
                    if (curEN.getPeerEpoch() < proposedEpoch) {
                        this.logger.fine("Ignoring notification from " + curEN.getSenderID() + " because its epoch is "
                                + curEN.getPeerEpoch() + ", which is earlier than " + proposedEpoch + ".");
                    } else if (curEN.getState() == OBSERVER) {
                        this.logger.fine("Ignoring notification from observer " + curEN.getSenderID());
                    } else {
                        //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
                        if (supersedesCurrentVote(curEN.getProposedLeaderID(), curEN.getPeerEpoch())) {
                            proposedEpoch = curEN.getPeerEpoch();
                            proposedLeader = curEN.getProposedLeaderID();
                            sendNotifications();
                            waitTime = STARTING_WAIT_TIME;
                        }
                        if (curEN.getProposedLeaderID() == proposedLeader) {
                            //(Be sure to keep track of the votes I received and who I received them from.)
                            receivedVotes.put(curEN.getSenderID(), curEN);
                            //If I have enough votes to declare my currently proposed leader as the leader...
                            if (haveEnoughVotes(receivedVotes, curEN)) {
                                //...do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
                                wait(finalizeWait); // Pretty sure this goes here??
                                boolean newFind = false;
                                for (
                                        ElectionNotification newEn = getNotificationFromMessage(incomingMessages.peek());
                                        newEn != null;
                                        incomingMessages.poll(),
                                        newEn = getNotificationFromMessage(incomingMessages.peek())
                                ) {
                                    if (supersedesCurrentVote(newEn.getProposedLeaderID(), newEn.getPeerEpoch())) {
                                        newFind = true;
                                        break;
                                    }
                                }
                                //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.
                                if (!newFind) {
                                    return acceptElectionWinner(curEN);
                                }
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            this.logger.log(Level.SEVERE,"Exception occurred during election; election canceled",e);
        }
        return null;
    }

    private void sendNotifications() {
        server.sendBroadcast(ELECTION, buildMsgContent(
                new ElectionNotification(proposedLeader, server.getPeerState(), server.getServerId(), proposedEpoch)
        ));
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        if (n.getProposedLeaderID() == server.getServerId()) {
            server.setPeerState(LEADING);
        } else {
            server.setPeerState(FOLLOWING);
        }
//        //clear out the incoming queue before returning
//        incomingMessages.clear(); // Not necessary for me because I poll when checking the queue
        return n;
    }

    /*
     * We return true if one of the following two cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal >= the size of my peer server’s quorum?
        Vote proposalVote = new Vote(proposal.getProposedLeaderID(), proposal.getPeerEpoch());
        if (votes.size() < server.getQuorumSize()) {
            return false;
        }
        int matches = 0;
        for (Vote v: votes.values()) {
            if (proposalVote.equals(v)) {
                matches++;
            }
        }
        return matches >= server.getQuorumSize();
    }

    public static ElectionNotification getNotificationFromMessage(Message msg){
        if (msg == null) {
            return null;
        }
        ByteBuffer readBuffer = ByteBuffer.wrap(msg.getMessageContents());
        return new ElectionNotification(readBuffer.getLong(), PeerServer.ServerState.getServerState(readBuffer.getChar()),
                readBuffer.getLong(), readBuffer.getLong());
    }

    /**
     * @return A 26-byte byte[].
     */
    public static byte[] buildMsgContent(ElectionNotification notification) {
        if (notification == null) {
            return null;
        }
        ByteBuffer writeBuffer = ByteBuffer.allocate(26);
        writeBuffer.putLong(notification.getProposedLeaderID());
        writeBuffer.putChar(notification.getState().getChar());
        writeBuffer.putLong(notification.getSenderID());
        writeBuffer.putLong(notification.getPeerEpoch());
        return writeBuffer.array();
    }
}