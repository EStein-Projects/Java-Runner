package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.Message.MessageType.COMPLETED_WORK;
import static edu.yu.cs.com3800.Message.MessageType.WORK;
import static edu.yu.cs.com3800.PeerServer.ServerState.FOLLOWING;
import static edu.yu.cs.com3800.PeerServer.ServerState.LOOKING;

public class JavaRunnerFollower implements Runnable, LoggingServer {
    private final PeerServer server;
    private final String myHost;
    private final int myTCPPort;
    private final Logger logger;

    public JavaRunnerFollower(PeerServer server) throws IOException {
        this.server = server;
        this.myHost = server.getAddress().getHostString();
        this.myTCPPort = server.getUdpPort() + 2;
        this.logger = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + this.myTCPPort);
        this.logger.fine("JavaRunnerFollower instance created");
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.fine("Thread started");
        try (ServerSocket socket = new ServerSocket(myTCPPort)) {
            while (server.getPeerState() == FOLLOWING && !Thread.currentThread().isInterrupted()) {
                try (
                        Socket leader = socket.accept();
                ) {
                    logger.fine("Accepted TCP connection from leader");
                    InputStream is = leader.getInputStream();
                    byte[] networkBytes = Util.readAllBytesFromNetwork(is);
                    if (networkBytes.length == 0) {
                        continue;
                    }
                    Message m = new Message(networkBytes);
                    if (m.getMessageType() != WORK) {
                        continue;
                    }
                    logger.fine("Received request #" + m.getRequestID()
                            + "from leader with tcp port: " + m.getSenderPort());
                    OutputStream os = leader.getOutputStream();
                    os.write(getResponse(m).getNetworkPayload());
                    os.flush();
                    logger.fine("Reply sent for request #" + m.getRequestID());
                }
            }
//        // Not sure about this
//        } catch (SocketTimeoutException | EOFException | SocketException e) {
//            logger.warning("Leader connection failed due to " + e + ".\nSetting server state to LOOKING");
//            try {
//                server.setCurrentLeader(null);
//            } catch (IOException ignored) {}
        } catch (IOException e) {
            logger.warning("IOException thrown");
            if (!Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
            }
        }
        logger.fine("Thread stopped");
    }

    private Message getResponse(Message m) {
        byte[] msgContents = m.getMessageContents();
        var bais = new ByteArrayInputStream(msgContents);
        try {
            byte[] result = new JavaRunner()
                    .compileAndRun(bais)
                    .getBytes();
            logger.fine("Successfully ran request #" + m.getRequestID());
            return new Message(
                    COMPLETED_WORK,
                    result,
                    myHost, myTCPPort, m.getSenderHost(), m.getSenderPort(), m.getRequestID(),
                    false
            );
        } catch (Exception e) {
            logger.fine("Exception thrown while running request #" + m.getRequestID());
            byte[] resultBeg = (e.getMessage() + "\n").getBytes();
            var baos = new ByteArrayOutputStream();
            var ps = new PrintStream(baos);
            e.printStackTrace(ps);
            ps.close();
            byte[] resultEnd = baos.toByteArray();
            byte[] result = new byte[resultBeg.length + resultEnd.length];
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
            return new Message(
                    COMPLETED_WORK,
                    result,
                    myHost, myTCPPort, m.getSenderHost(), m.getSenderPort(), m.getRequestID(),
                    true
            );
        }
    }
}