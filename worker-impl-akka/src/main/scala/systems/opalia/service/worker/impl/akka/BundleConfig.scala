package systems.opalia.service.worker.impl.akka

import com.typesafe.config.{Config, ConfigFactory}
import java.nio.file.Path
import scala.concurrent.duration._
import systems.opalia.commons.configuration.ConfigHelper._
import systems.opalia.commons.configuration.Reader._
import systems.opalia.commons.net.EndpointAddress
import systems.opalia.interfaces.logging.LogLevel


final class BundleConfig(config: Config) {

  val logLevel: LogLevel = LogLevel.min(config.as[LogLevel]("log.level"), LogLevel.INFO)

  val nativeLibraryPath: Path = config.as[Path]("worker.native-library-path").toAbsolutePath.normalize

  val frontendServer: EndpointAddress = config.as[EndpointAddress]("worker.frontend-server")
  val backendServer: EndpointAddress = config.as[EndpointAddress]("worker.backend-server")
  val seedNodes: List[EndpointAddress] = config.as[List[EndpointAddress]]("worker.seed-nodes")

  val akka: Config =
    ConfigFactory.parseString(
      s"""
         |akka {
         |  loglevel = "$logLevel"
         |
         |  actor {
         |    provider = cluster
         |
         |    serializers {
         |      java = "akka.serialization.JavaSerializer"
         |      proto = "akka.remote.serialization.ProtobufSerializer"
         |      custom = "${classOf[MessageSerializer].getName}"
         |    }
         |
         |    serialization-bindings {
         |      "${classOf[RemoteRequest].getName}" = custom
         |      "${classOf[RemoteResponse].getName}" = custom
         |    }
         |  }
         |
         |  cluster {
         |    jmx.multi-mbeans-in-same-jvm = on
         |    metrics.enabled = off
         |
         |    metrics.native-library-extract-folder = $nativeLibraryPath
         |
         |    seed-nodes = [ ${seedNodes.map("\"akka.tcp://ClusterSystem@" + _ + "\"").mkString(",")} ]
         |  }
         |
         |  extensions = ["akka.cluster.metrics.ClusterMetricsExtension"]
         |}
       """.stripMargin)
      .withFallback(config.as[Config]("worker").withOnlyPath("akka"))

  val akkaFrontend: Config =
    ConfigFactory.parseString("akka.cluster.roles = [frontend]")
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname = ${frontendServer.hostString}"))
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port = ${frontendServer.port}"))
      .withFallback(akka)

  val akkaBackend: Config =
    ConfigFactory.parseString("akka.cluster.roles = [backend]")
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname = ${backendServer.hostString}"))
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port = ${backendServer.port}"))
      .withFallback(akka)

  val processTimeout: FiniteDuration = config.as[FiniteDuration]("worker.process-timeout")
}
