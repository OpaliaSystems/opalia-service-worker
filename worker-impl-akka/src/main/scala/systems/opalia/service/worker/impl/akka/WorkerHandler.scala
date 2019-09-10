package systems.opalia.service.worker.impl.akka

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.util.Timeout
import java.util.concurrent.TimeoutException
import scala.concurrent.{ExecutionContext, Future, Promise}
import systems.opalia.interfaces.logging.Logger
import systems.opalia.interfaces.worker.Message


final class WorkerHandler(config: BundleConfig,
                          logger: Logger,
                          loggerStats: Logger,
                          actorSystemFrontend: ActorSystem,
                          actorSystemBackend: ActorSystem,
                          consumerRegistry: ConsumerRegistry,
                          feedbackRegistry: FeedbackRegistry)
                         (implicit executionContext: ExecutionContext) {

  private val backendLocal =
    actorSystemBackend.actorOf(WorkerBackend.props(
      config,
      logger,
      loggerStats,
      consumerRegistry
    ), "workerBackend")

  private val frontend =
    actorSystemFrontend.actorSelection("user/workerFrontend")

  private val memberUpPromise =
    Promise[Unit]()

  val memberUpFuture: Future[Unit] =
    memberUpPromise.future

  Cluster(actorSystemFrontend).registerOnMemberUp {

    actorSystemFrontend.actorOf(WorkerFrontend.props(
      config,
      logger,
      loggerStats,
      backendLocal,
      feedbackRegistry
    ), "workerFrontend")

    actorSystemFrontend.actorOf(ClusterListener.props(
      config,
      logger,
      loggerStats
    ), "metricsListener")

    memberUpPromise.success({})
  }

  def send(topic: String, message: Message, local: Boolean)
          (implicit timeout: Timeout): Future[Message] = {

    val promise = Promise[Message]()
    val channelId = feedbackRegistry.waitFor(promise)

    actorSystemFrontend.scheduler.scheduleOnce(timeout.duration) {

      feedbackRegistry.remove(channelId)

      val error =
        new TimeoutException(s"Execution timeout after ${timeout.duration.toMillis} ms.")

      if (promise.tryFailure(error))
        logger.warning("Unexpected timeout occurred during worker execution.", error)
    }

    if (local)
      frontend ! Local(RemoteRequest(channelId, topic, message))
    else
      frontend ! RemoteRequest(channelId, topic, message)

    promise.future
  }
}
