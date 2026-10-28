package edu.yu.cs.com3800.stage1;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;
import edu.yu.cs.com3800.stage1.Client.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

class Stage1Test {
    private static final int PORT = 9000;
    private static final String SCHEME = "HTTP";
    private static final String HELLO_WORLD_CLASS = "public class HelloWorld {" +
            "public HelloWorld(){}" +
            "public String run() {" +
                "return \"Hello world\";" +
            "}" +
    "}";
    private static SimpleServer server;
    private static Client client;
    private static HttpClient httpClient;

    @BeforeAll
    static void setup() throws IOException {
        server = new SimpleServerImpl(PORT);
        server.start();
        client = new ClientImpl("localhost", 9000);
        httpClient = HttpClient.newHttpClient();
    }

    @AfterAll
    static void cleanup() {
        server.stop();
    }

    @Test
    void wrongPathClient() throws URISyntaxException, IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(
                SCHEME, null, "localhost", PORT,
                "/", null, null
        ));
        HttpResponse<String> response = httpClient.send(
                builder.setHeader("Content-Type", "text/x-java-source")
                        .PUT(HttpRequest.BodyPublishers.ofString(HELLO_WORLD_CLASS))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );

        int expectedCode = 404;
        int actualCode = response.statusCode();
        System.out.println("\nWrong path client:\n");
        printComparison(expectedCode, actualCode);
        assertEquals(expectedCode, actualCode);
    }

    @Test
    void failedPutRequestClient() throws URISyntaxException, IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(
                SCHEME, null, "localhost", PORT,
                "/compileandrun", null, null
        ));
        HttpResponse<String> response = httpClient.send(
                builder.setHeader("Content-Type", "text/x-java-source")
                        .PUT(HttpRequest.BodyPublishers.ofString(HELLO_WORLD_CLASS))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );

        int expectedCode = 405;
        int actualCode = response.statusCode();
        System.out.println("\nFailed put request client:\n");
        printComparison(expectedCode, actualCode);
        assertEquals(expectedCode, actualCode);
    }

    @Test
    void badHeaderClient() throws URISyntaxException, IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(
                SCHEME, null, "localhost", PORT,
                "/compileandrun", null, null
        ));
        HttpResponse<String> response = httpClient.send(
                builder.setHeader("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(HELLO_WORLD_CLASS))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );

        int expectedCode = 400;
        int actualCode = response.statusCode();
        System.out.println("\nBad header client:\n");
        printComparison(expectedCode, actualCode);
        assertEquals(expectedCode, actualCode);
    }

    @Test
    void unrunnableJavaRunner() throws IOException {
        String unrunnableClass = "public class HelloWorld {" +
                "public HelloWorld(){}" +
                "public String print() {" +
                    "System.out.println(\"Hello world\");" +
                "}" +
        "}";
        client.sendCompileAndRunRequest(unrunnableClass);
        Exception e = assertThrows(Exception.class, () ->
                new JavaRunner().compileAndRun(
                        new ByteArrayInputStream(unrunnableClass.getBytes())
                )
        );
        var baos = new ByteArrayOutputStream();
        var ps = new PrintStream(baos);
        e.printStackTrace(ps);
        ps.close();
        String expectedResult = e.getMessage() + "\n" + baos;
        Response expectedResponse = new Response(400, expectedResult);
        Response actualResponse = client.getResponse();

        System.out.println("\nUnrunnable JavaRunner:\n");
        checkResponse(expectedResponse, actualResponse);
    }

    @Test
    void clientServerHelloWorld() throws IOException, ReflectiveOperationException {
        client.sendCompileAndRunRequest(HELLO_WORLD_CLASS);
        Response expectedResponse = new Response(200,
                new JavaRunner().compileAndRun(
                        new ByteArrayInputStream(HELLO_WORLD_CLASS.getBytes())
                )
        );
        Response actualResponse = client.getResponse();

        System.out.println("\nClient server hello world:\n");
        checkResponse(expectedResponse, actualResponse);
    }

    @Test
    void ArgumentsTests() {
        assertAll(
                () -> {
                    assertThrowsExactly(IllegalArgumentException.class, () -> new SimpleServerImpl(-1));
                    assertThrowsExactly(IllegalArgumentException.class, () -> new SimpleServerImpl(65536));
                    new SimpleServerImpl(0);
                    new SimpleServerImpl(65535);
                },
                () -> {
                    assertThrowsExactly(IllegalArgumentException.class, () -> new ClientImpl(null, 9000));
                    assertThrowsExactly(IllegalArgumentException.class, () -> new ClientImpl("", 9000));
                    assertThrowsExactly(IllegalArgumentException.class, () -> new ClientImpl("localhost", -1));
                    assertThrowsExactly(IllegalArgumentException.class, () -> new ClientImpl("localhost", 65536));
                    new ClientImpl("localhost", 0);
                    new ClientImpl("localhost", 65535);
                }
        );
        System.out.println("\nArguments tests passed.\n");
    }

    @Test
    void OutOfOrderClient() {
        assertThrows(Exception.class, () -> new ClientImpl("localhost", 9000).getResponse());
        System.out.println("\nOut of order client test passed.\n");
    }

    private void checkResponse(Response expected, Response actual) {
        int expectedCode = expected.getCode();
        int actualCode = actual.getCode();
        String expectedBody = getTrace(expected.getBody());
        String actualBody = getTrace(actual.getBody());
        printComparison(
                expectedCode + "\t" + expectedBody,
                actualCode + "\t" + actualBody
        );
        assertAll(
                () -> {
                    assertEquals(expectedCode, actualCode);
                },
                () -> {
                    assertEquals(expectedBody, actualBody);
                }
        );
    }

    private void printComparison(int expected, int actual) {
        System.out.println("Expected response:\n" + expected +
                "\nActual response:\n" + actual
        );
    }

    private void printComparison(String expected, String actual) {
        System.out.println("Expected response:\n" + expected +
                "\nActual response:\n" + actual
        );
    }

    private String getTrace(String entireBody) {
        int i = entireBody.indexOf("68)");
        if (i == -1) {
            return entireBody;
        }
        return entireBody.substring(0, i+3);
    }
}