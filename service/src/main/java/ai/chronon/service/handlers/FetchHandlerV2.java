package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.online.fetcher.FeaturesResponseType;
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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Failure;
import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Success;

public class FetchHandlerV2 implements Handler<RoutingContext> {
    private static final Logger logger = LoggerFactory.getLogger(FetchHandlerV2.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // W3C TraceContext header extractor for Vert.x routing contexts.
    private static final TextMapGetter<RoutingContext> TRACE_CONTEXT_GETTER =
            new TextMapGetter<RoutingContext>() {
                @Override
                public Iterable<String> keys(RoutingContext carrier) {
                    return carrier.request().headers().names();
                }
                @Nullable
                @Override
                public String get(@Nullable RoutingContext carrier, String key) {
                    return carrier == null ? null : carrier.request().headers().get(key);
                }
            };

    private final JavaFetcher fetcher;
    private final BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> fetchFunction;
    private final BiFunction<List<GetFeaturesResponse.Result>, RoutingContext, Void> onSuccessFunction;

    public FetchHandlerV2(JavaFetcher fetcher,
                          BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> fetchFunction,
                          BiFunction<List<GetFeaturesResponse.Result>, RoutingContext, Void> onSuccessFunction
    ) {
        this.fetcher = fetcher;
        this.fetchFunction = fetchFunction;
        this.onSuccessFunction = onSuccessFunction;
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

        JTry<List<JavaRequest>> maybeRequest = parseJavaRequest(entityName, ctx.body());

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
                                        .map(FetchHandlerV2::responseToPoJo)
                                        .collect(Collectors.toList()));

        maybeFeatureResponses.onSuccess(resultList -> {
            scope.close();
            span.setStatus(StatusCode.OK);
            span.end();
            onSuccessFunction.apply(resultList, ctx);
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
        var valueType = response.valueType;
        var builder = GetFeaturesResponse.Result.builder()
                .entityKeys(response.request.keys);

        builder.featuresErrors(response.errorsV2.getValue());

        // Get the appropriate JTry and set up success builder based on response type
        JTry featureValues;
        if (valueType == FeaturesResponseType.Map()) {
            featureValues = response.values;
            if (featureValues.isSuccess()) {
                return builder.status(Success).features((Map<String, Object>) featureValues.getValue()).build();
            }
        } else if (valueType == FeaturesResponseType.AvroString()) {
            featureValues = response.valuesAvroString;
            if (featureValues.isSuccess()) {
                return builder.status(Success).featureAvroString((String) featureValues.getValue()).build();
            }
        } else {
            return builder.status(Failure).error("Unknown response type: " + valueType).build();
        }

        // Handle failure case (featureValues.isSuccess() was false)
        return builder.status(Failure).error(featureValues.getException().getMessage()).build();
    }

    public static JTry<List<JavaRequest>> parseJavaRequest(String name, RequestBody body) {

        TypeReference<List<Map<String, Object>>> ref = new TypeReference<List<Map<String, Object>>>() { };

        try {

            List<Map<String, Object>> entityKeysList = objectMapper.readValue(body.asString(), ref);

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