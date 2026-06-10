package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.online.metrics.OtelTracing;
import ai.chronon.service.model.GetFeaturesResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Failure;
import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Success;

/**
 * Concrete implementation of the Chronon fetcher endpoints. Supports loading groupBys and joins.
 * Some notes on this:
 * We currently support bulkGet lookups against a single groupBy / join. Attempts to lookup n different GroupBys / Joins
 * need to be split up into n different requests.
 * A given bulkGet request might result in some successful lookups and some failed ones. We return a 4xx or 5xx response
 * if the overall request fails (e.g. we're not able to parse the input json, Future failure due to Api returning an error)
 * Individual failure responses will be marked as 'Failed' however the overall response status code will be successful (200)
 * The response list maintains the same order as the incoming request list.
 * As an example:
 * { results: [ {"status": "Success", "features": ...}, {"status": "Failure", "error": ...} ] }
 */
public class FetchHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(FetchHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // W3C TraceContext header extractor for Vert.x routing contexts.
    private static final TextMapGetter<RoutingContext> TRACE_CONTEXT_GETTER =
            new TextMapGetter<RoutingContext>() {
                @Override
                public Iterable<String> keys(RoutingContext carrier) {
                    return carrier.request().headers().names();
                }
                @Override
                public String get(RoutingContext carrier, String key) {
                    return carrier == null ? null : carrier.request().headers().get(key);
                }
            };

    private final JavaFetcher fetcher;
    private final BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> fetchFunction;

    public FetchHandler(JavaFetcher fetcher, BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> fetchFunction) {
        this.fetcher = fetcher;
        this.fetchFunction = fetchFunction;
    }

    @Override
    public void handle(RoutingContext ctx) {

        String entityName = ctx.pathParam("name");

        logger.debug("Retrieving {}", entityName);

        // Extract incoming W3C trace context so distributed traces stitch correctly across
        // service boundaries. If no traceparent header is present this produces a root context.
        OtelTracing otelTracing = OtelTracing.instance();
        Context parentContext = otelTracing.openTelemetry()
                .getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), ctx, TRACE_CONTEXT_GETTER);

        Span span = otelTracing.openTelemetry()
                .getTracer("ai.chronon")
                .spanBuilder("http.fetch")
                .setParent(parentContext)
                .setSpanKind(SpanKind.SERVER)
                .setAttribute(AttributeKey.stringKey("chronon.entity.name"), entityName)
                .setAttribute(AttributeKey.stringKey("http.method"), ctx.request().method().name())
                .startSpan();
        Scope scope = span.makeCurrent();

        String requestBody = ctx.body().asString();
        JTry<List<JavaRequest>> maybeRequest = parseJavaRequest(entityName, requestBody);

        if (! maybeRequest.isSuccess()) {

            logger.error("Unable to parse request body", maybeRequest.getException());
            scope.close();
            span.recordException(maybeRequest.getException());
            span.setStatus(StatusCode.ERROR);
            span.end();

            List<String> errorMessages = Collections.singletonList(maybeRequest.getException().getMessage());

            ctx.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("errors", errorMessages).encode());

            return;
        }

        List<JavaRequest> requests = maybeRequest.getValue();
        CompletableFuture<List<JavaResponse>> resultsJavaFuture = fetchFunction.apply(fetcher, requests);

        // wrap the Java future we get in a Vert.x Future to not block the worker thread
        Future<List<GetFeaturesResponse.Result>> maybeFeatureResponses =
                Future.fromCompletionStage(resultsJavaFuture)
                      .map(result ->
                              result.stream()
                                      .map(FetchHandler::responseToPoJo)
                                      .collect(Collectors.toList()));

        maybeFeatureResponses.onSuccess(
                resultList -> {
                    scope.close();
                    span.setStatus(StatusCode.OK);
                    span.end();

                    // as this is a bulkGet request, we might have some successful and some failed responses
                    // we return the responses in the same order as they come in and mark them as successful / failed based
                    // on the lookups
                    GetFeaturesResponse.Builder responseBuilder = GetFeaturesResponse.builder();
                    responseBuilder.results(resultList);

                    ctx.response()
                            .setStatusCode(200)
                            .putHeader("content-type", "application/json")
                            .end(JsonObject.mapFrom(responseBuilder.build()).encode());
                });

        maybeFeatureResponses.onFailure(
                err -> {
                    scope.close();
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR);
                    span.end();

                    List<String> failureMessages = Collections.singletonList(err.getMessage());

                    ctx.response()
                            .setStatusCode(500)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("errors", failureMessages).encode());
                });
    }

    public static GetFeaturesResponse.Result responseToPoJo(JavaResponse response) {

        if (response.values.isSuccess()) {

            return GetFeaturesResponse.Result
                    .builder()
                    .status(Success)
                    .entityKeys(response.request.keys)
                    .features(response.values.getValue())
                    .build();
        } else {

            return GetFeaturesResponse.Result
                    .builder()
                    .status(Failure)
                    .entityKeys(response.request.keys)
                    .error(response.values.getException().getMessage())
                    .build();
        }
    }

    public static JTry<List<JavaRequest>> parseJavaRequest(String name, String requestPayload) {

        TypeReference<List<Map<String, Object>>> ref = new TypeReference<List<Map<String, Object>>>() { };

        try {

            List<Map<String, Object>> entityKeysList = objectMapper.readValue(requestPayload, ref);

            List<JavaRequest> requests = entityKeysList
                    .stream()
                    .map(m -> new JavaRequest(name, m))
                    .collect(Collectors.toList());

            return JTry.success(requests);

        } catch (Exception e) {
            return JTry.failure(e);
        }
    }
}
