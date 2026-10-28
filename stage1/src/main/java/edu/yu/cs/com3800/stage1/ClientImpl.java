package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ClientImpl implements Client {
    private static final String SCHEME = "HTTP";
    private final HttpClient client;
    private final HttpRequest.Builder builder;
    private HttpResponse<String> lastResponse;

    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        if (hostName == null || hostName.isEmpty()) {
            throw new IllegalArgumentException("Invalid hostname.");
        }
        if (hostPort < 0 || hostPort > 65535) {
            throw new IllegalArgumentException(
                    "Invalid hostport. It must be between 0 and 65535, inclusive."
            );
        }
        client = HttpClient.newHttpClient();
        try {
            builder = HttpRequest.newBuilder(new URI(
                    SCHEME, null, hostName, hostPort,
                    "/compileandrun", null, null
            ));
        } catch (URISyntaxException e) {
            throw new MalformedURLException(e.getMessage());
        }
    }

    @Override
    public void sendCompileAndRunRequest(String src) throws IOException {
        try {
            lastResponse = client.send(builder
                            .setHeader("Content-Type", "text/x-java-source")
                            .POST(HttpRequest.BodyPublishers.ofString(src))
                            .build(),
                    HttpResponse.BodyHandlers.ofString()
            );
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Response getResponse() throws IOException {
        if (lastResponse == null) {
            throw new IOException("Response has not been received yet. Have you sent a request?");
        }
        return new Response(lastResponse.statusCode(), lastResponse.body());
    }
}
