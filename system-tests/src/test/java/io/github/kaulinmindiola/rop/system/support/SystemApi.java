package io.github.kaulinmindiola.rop.system.support;

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * The public HTTP API, used exactly as a client would use it. No code is shared with the services.
 */
public final class SystemApi {

    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final String baseUrl;

    public SystemApi(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String obtainToken(String clientId, String clientSecret) {
        HttpResponse<String> response =
                send(
                        HttpRequest.newBuilder(URI.create(baseUrl + "/auth/token"))
                                .header("Content-Type", "application/json")
                                .POST(
                                        HttpRequest.BodyPublishers.ofString(
                                                """
                            {"clientId":"%s","clientSecret":"%s"}
                            """
                                                        .formatted(clientId, clientSecret)))
                                .build());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Token request failed: " + response.statusCode());
        }
        return JsonPath.read(response.body(), "$.accessToken");
    }

    public HttpResponse<String> createOrder(String token, String productId, int quantity) {
        return send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/orders"))
                        .header("Authorization", "Bearer " + token)
                        .header("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        """
                        {"items":[{"productId":"%s","quantity":%d}]}
                        """
                                                .formatted(productId, quantity)))
                        .build());
    }

    /** The order id taken from the Location header of a 201 response. */
    public static String orderIdOf(HttpResponse<String> created) {
        String location =
                created.headers()
                        .firstValue("Location")
                        .orElseThrow(() -> new IllegalStateException("No Location header"));
        return location.substring(location.lastIndexOf('/') + 1);
    }

    public String statusOf(String token, String orderId) {
        HttpResponse<String> response =
                send(
                        HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/orders/" + orderId))
                                .header("Authorization", "Bearer " + token)
                                .GET()
                                .build());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("GET order failed: " + response.statusCode());
        }
        return JsonPath.read(response.body(), "$.status");
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while calling the API", e);
        }
    }
}
