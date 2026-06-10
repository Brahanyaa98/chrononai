package ai.chronon.online.metrics

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.{Span, StatusCode, Tracer}
import io.opentelemetry.context.{Context, Scope}
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
import org.slf4j.LoggerFactory

import java.util.concurrent.Executor
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Thin wrapper around the OTel [[Tracer]] that provides Scala-friendly span helpers and
  * handles async context propagation across [[Future]] thread hops.
  *
  * Obtain via [[OtelTracing.instance]] — do not construct directly in production code.
  */
class OtelTracing(val openTelemetry: OpenTelemetry) {

  private val tracer: Tracer = openTelemetry
    .tracerBuilder("ai.chronon")
    .setInstrumentationVersion("0.0.0")
    .build()

  /** Wrap an [[ExecutionContext]] so every task submitted to it captures and restores the current
    * OTel [[Context]]. Without this, Future callbacks running on a thread pool would lose the
    * active span, breaking parent-child span relationships across async boundaries.
    */
  def contextPropagatingEc(ec: ExecutionContext): ExecutionContext = {
    val wrappedExecutor: Executor = Context.taskWrapping(ec.execute _)
    ExecutionContext.fromExecutor(wrappedExecutor)
  }

  /** Run a Future-producing block inside a named span. The span is ended (OK or ERROR) when the
    * returned Future settles. The [[Scope]] is closed synchronously after `f()` returns, so child
    * spans started synchronously inside `f` correctly inherit the parent — deeper async children
    * rely on a context-propagating EC (see [[contextPropagatingEc]]).
    */
  def withSpan[T](spanName: String, attributes: Attributes = Attributes.empty())(
      f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val span: Span = tracer.spanBuilder(spanName).setAllAttributes(attributes).startSpan()
    val scope: Scope = span.makeCurrent()
    val fut =
      try { f }
      catch {
        case t: Throwable =>
          scope.close()
          span.recordException(t)
          span.setStatus(StatusCode.ERROR)
          span.end()
          return Future.failed(t)
      }
    scope.close()
    fut.transform {
      case s @ Success(_) =>
        span.setStatus(StatusCode.OK)
        span.end()
        s
      case f @ Failure(t) =>
        span.recordException(t)
        span.setStatus(StatusCode.ERROR)
        span.end()
        f
    }
  }

  /** Synchronous span helper — starts a span, runs `f`, then ends the span. */
  def withSpanSync[T](spanName: String, attributes: Attributes = Attributes.empty())(f: => T): T = {
    val span: Span = tracer.spanBuilder(spanName).setAllAttributes(attributes).startSpan()
    val scope: Scope = span.makeCurrent()
    try {
      val result = f
      span.setStatus(StatusCode.OK)
      result
    } catch {
      case t: Throwable =>
        span.recordException(t)
        span.setStatus(StatusCode.ERROR)
        throw t
    } finally {
      scope.close()
      span.end()
    }
  }
}

object OtelTracing {

  private val logger = LoggerFactory.getLogger(classOf[OtelTracing])

  // ---- Config keys (mirror the ai.chronon.metrics.* pattern) ----
  val TracingEnabled = "ai.chronon.tracing.enabled"
  val TracingExporterUrlKey = "ai.chronon.tracing.exporter.url"
  val TracingExporterProtocolKey = "ai.chronon.tracing.exporter.protocol"

  // Module-level singleton — initialised during Metrics.Context init so it shares the same
  // OpenTelemetry SDK instance as the metrics reporter. Defaults to noop so callers are safe
  // before Metrics is first accessed.
  @volatile private[metrics] var globalInstance: OtelTracing = new OtelTracing(OpenTelemetry.noop())

  /** Returns the active [[OtelTracing]] singleton.
    * Also accessible from Java as the static method OtelTracing.instance().
    */
  def instance: OtelTracing = globalInstance

  def isEnabled: Boolean = System.getProperty(TracingEnabled, "false").toBoolean

  /** Build a [[SdkTracerProvider]] using config aligned with the metrics exporter config.
    * Called from [[OtelMetricsReporter.buildOpenTelemetryClient]] so both signal providers
    * share the same [[Resource]] and [[io.opentelemetry.sdk.OpenTelemetrySdk]] instance.
    */
  def buildTracerProvider(resource: Resource): SdkTracerProvider = {
    val protocol = System.getProperty(TracingExporterProtocolKey, "http").toLowerCase
    val exporterUrl = System.getProperty(TracingExporterUrlKey, OtelMetricsReporter.MetricsExporterUrlDefault)

    val spanExporter = protocol match {
      case "grpc" =>
        OtlpGrpcSpanExporter.builder().setEndpoint(exporterUrl).build()
      case _ =>
        OtlpHttpSpanExporter.builder().setEndpoint(s"$exporterUrl/v1/traces").build()
    }

    logger.info(s"Building OTel tracer provider: protocol=$protocol, url=$exporterUrl")

    SdkTracerProvider.builder()
      .setResource(resource)
      .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
      .build()
  }
}
