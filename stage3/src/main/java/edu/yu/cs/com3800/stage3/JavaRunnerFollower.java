package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.COMPLETED_WORK;
import static edu.yu.cs.com3800.Message.MessageType.WORK;
import static edu.yu.cs.com3800.PeerServer.ServerState.FOLLOWING;

public class JavaRunnerFollower implements Runnable, LoggingServer {
    private final PeerServer server;
    private final String myHost;
    private final int myPort;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private Logger logger;

    public JavaRunnerFollower(
            PeerServer server, LinkedBlockingQueue<Message> outgoingMessages, LinkedBlockingQueue<Message> incomingMessages
    ) {
        this.server = server;
        this.myHost = server.getAddress().getHostString();
        this.myPort = server.getUdpPort();
        this.outgoingMessages = outgoingMessages;
        this.incomingMessages = incomingMessages;
        try {
            this.logger = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + server.getUdpPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.fine("Thread started");
        while (server.getPeerState() == FOLLOWING && !Thread.currentThread().isInterrupted()) {
            try {
                Message m = incomingMessages.take();
                if (m.getMessageType() != WORK) {
                    continue;
                }
                logger.fine("Received request #" + m.getRequestID());
                byte[] msgContents = m.getMessageContents();
                byte[] result;
                var bais = new ByteArrayInputStream(msgContents);
                try {
                    result = new JavaRunner()
                            .compileAndRun(bais)
                            .getBytes();
                } catch (Exception e) {
                    byte[] resultBeg = (e.getMessage() + "\n").getBytes();
                    var baos = new ByteArrayOutputStream();
                    var ps = new PrintStream(baos);
                    e.printStackTrace(ps);
                    ps.close();
                    byte[] resultEnd = baos.toByteArray();
                    result = new byte[resultBeg.length + resultEnd.length];
                    System.arraycopy(
                            resultBeg, 0,
                            result, 0,
                            resultBeg.length
                    );
                    System.arraycopy(
                            resultEnd, 0,
                            result, resultBeg.length,
                            resultEnd.length
                    );
                }
                outgoingMessages.put(new Message(
                        COMPLETED_WORK, result, myHost, myPort, m.getSenderHost(), m.getSenderPort(), m.getRequestID()
                ));
                logger.fine("Completed request #" + m.getRequestID());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
