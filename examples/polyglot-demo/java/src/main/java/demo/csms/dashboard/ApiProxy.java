package demo.csms.dashboard;

import demo.csms.config.ConditionalOnRole;
import demo.csms.config.CsmsProperties;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

/**
 * When the dashboard runs without the API role, forwards the page's {@code /api/...} calls to
 * the command API service, so the browser keeps a single origin.
 */
@RestController
@ConditionalOnRole(value = ConditionalOnRole.DASHBOARD, unless = ConditionalOnRole.API)
public class ApiProxy {

    private final RestClient client;

    public ApiProxy(RestClient.Builder builder, CsmsProperties properties) {
        this.client = builder.baseUrl(properties.commandApiUrl()).build();
    }

    @RequestMapping("/api/**")
    public ResponseEntity<byte[]> forward(HttpMethod method, HttpServletRequest request,
                                          @RequestBody(required = false) byte[] body) {
        String uri = request.getRequestURI() + (request.getQueryString() == null ? "" : "?" + request.getQueryString());
        RestClient.RequestBodySpec spec = client.method(method).uri(uri);
        if (request.getContentType() != null) {
            spec.header(HttpHeaders.CONTENT_TYPE, request.getContentType());
        }
        if (body != null) {
            spec.body(body);
        }
        return spec.exchange((req, res) -> ResponseEntity.status(res.getStatusCode())
                .headers(h -> h.addAll(HttpHeaders.CONTENT_TYPE, res.getHeaders().getOrEmpty(HttpHeaders.CONTENT_TYPE)))
                .body(res.getBody().readAllBytes()));
    }
}
