package edu.yu.cs.com3800.stage1;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandlers;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;

public class SimpleServerImpl implements SimpleServer {
    private final HttpServer server;

    public SimpleServerImpl(int port) throws IOException {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "Invalid port. It must be between 0 and 65535, inclusive."
            );
        }
        server = HttpServer.create(
                new InetSocketAddress(port), 0, "/compileandrun",
                HttpHandlers.handleOrElse(
                        request -> request
                                .getRequestMethod()
                                .equalsIgnoreCase("POST"),
                        HttpHandlers.handleOrElse(
                                request -> request
                                        .getRequestHeaders()
                                        .get("Content-Type")
                                        .contains("text/x-java-source"),
                                exchange -> {
                                    int responseCode = 200;
                                    byte[] responseBody;
                                    try {
                                        responseBody = new JavaRunner()
                                                .compileAndRun(exchange.getRequestBody())
                                                .getBytes();
                                    } catch (Exception e) {
                                        responseCode = 400;
                                        byte[] resultBeg = (e.getMessage() + "\n").getBytes();
                                        var baos = new ByteArrayOutputStream();
                                        var ps = new PrintStream(baos);
                                        e.printStackTrace(ps);
                                        ps.close();
                                        byte[] resultEnd = baos.toByteArray();
                                        responseBody = new byte[resultBeg.length + resultEnd.length];
                                        System.arraycopy(
                                                resultBeg, 0,
                                                responseBody, 0,
                                                resultBeg.length
                                        );
                                        System.arraycopy(
                                                resultEnd, 0,
                                                responseBody, resultBeg.length,
                                                resultEnd.length
                                        );
                                    }
                                    exchange.sendResponseHeaders(responseCode, responseBody.length);
                                    // Javadoc on HttpExchange#getResponseBody():
                                    // Closing this stream implicitly closes the InputStream
                                    // returned from getRequestBody() (if it is not already closed).
                                    try (OutputStream os = exchange.getResponseBody()) {
                                        os.write(responseBody);
                                    }
                                },
                                HttpHandlers.of(400, new Headers(), "")
                        ),
                        HttpHandlers.of(405, new Headers(), "")
                )
//                , Filter.afterHandler("Log response code", e -> log(e.getResponseCode()))
        );
    }

    /**
     * start the server
     */
    @Override
    public void start() {
        server.start();
    }

    /**
     * stop the server
     */
    @Override
    public void stop() {
        server.stop(0);
    }

    public static void main(String[] args) {
        int port = 9000;
        if(args.length > 0)
        {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try
        {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        }
        catch(Exception e)
        {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }
}
