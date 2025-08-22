package software.amazon.ssa.streams.connector;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.net.InetSocketAddress;
import java.io.OutputStream;

public class HealthServer {
    public static void start() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/health", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) {
                try {
                    String response = "OK";
                    exchange.sendResponseHeaders(200, response.length());
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        server.setExecutor(null); // single-threaded
        server.start();
    }
}