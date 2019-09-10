package systems.opalia.service.worker.impl.akka

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.util.Timeout
import kamon.sigar.SigarProvisioner
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import systems.opalia.interfaces.logging.LoggingService
import systems.opalia.interfaces.soa.Bootable
import systems.opalia.interfaces.worker._


final class WorkerServiceBootable(config: BundleConfig,
                                  classLoader: ClassLoader,
                                  loggingService: LoggingService)
  extends WorkerService
    with Bootable[Unit, Unit] {

  provisionNativeLibraries(config)

  private val logger = loggingService.newLogger(classOf[WorkerService].getName)
  private val loggerStats = loggingService.newLogger(s"${classOf[WorkerService].getName}-statistics")

  private val actorSystemBackend = ActorSystem("ClusterSystem", config.akkaBackend, classLoader)
  private val actorSystemFrontend = ActorSystem("ClusterSystem", config.akkaFrontend, classLoader)

  private implicit val executionContext = actorSystemFrontend.dispatcher

  private implicit val timeout = Timeout(config.processTimeout)

  private val consumerRegistry = new ConsumerRegistry()
  private val feedbackRegistry = new FeedbackRegistry()

  private val workerHandler =
    new WorkerHandler(
      config,
      logger,
      loggerStats,
      actorSystemFrontend,
      actorSystemBackend,
      consumerRegistry,
      feedbackRegistry
    )

  def newProducer(_topic: String, local: Boolean): Producer =
    new Producer {

      def topic: String =
        _topic

      def syncCall(message: Message): Message =
        Await.result(asyncCall(message), Duration.Inf)

      def asyncCall(message: Message): Future[Message] =
        workerHandler.send(_topic, message, local)
    }

  def newConsumer(_topic: String, consume: (Message) => Message): Consumer = {
    new Consumer {

      def topic: String =
        _topic

      def apply(message: Message): Message =
        consume(message)

      def register(): Unit =
        consumerRegistry.add(this)

      def unregister(): Unit =
        consumerRegistry.remove(_topic)
    }
  }

  def registerObjectMapper(objectMapper: ObjectMapper): Unit = {

    GlobalObjectMapperRegistry.add(objectMapper)
  }

  def unregisterObjectMapper(id: Int): Unit = {

    GlobalObjectMapperRegistry.remove(id)
  }

  protected def setupTask(): Unit = {

    Await.result(workerHandler.memberUpFuture, Duration.Inf)
  }

  protected def shutdownTask(): Unit = {

    val cluster = Cluster(actorSystemBackend)

    cluster.leave(cluster.selfAddress)

    val future1 = actorSystemFrontend.terminate()
    val future2 = actorSystemBackend.terminate()

    Await.result(
      for {
        _1 <- future1
        _2 <- future2
      } yield (_1, _2),
      Duration.Inf)

    consumerRegistry.clear()
    feedbackRegistry.clear()
    GlobalObjectMapperRegistry.clear()
  }

  private def provisionNativeLibraries(config: BundleConfig): Unit = {

    sys.props("java.library.path") = s"${sys.props("java.library.path")}:${config.nativeLibraryPath}"

    SigarProvisioner.provision(config.nativeLibraryPath.toFile)
  }
}
