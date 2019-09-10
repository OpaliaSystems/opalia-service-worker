package systems.opalia.service.worker.impl.akka

import akka.actor._
import akka.routing.FromConfig
import java.time.{Duration, Instant}
import systems.opalia.interfaces.logging.Logger


final class WorkerFrontend(config: BundleConfig,
                           logger: Logger,
                           loggerStats: Logger,
                           backendLocal: ActorRef,
                           feedbackRegistry: FeedbackRegistry)
  extends Actor {

  private val backend = context.actorOf(FromConfig.props(), "workerBackendRouter")

  def receive = {

    case Local(request: RemoteRequest) =>
      backendLocal ! request

    case request: RemoteRequest =>
      backend ! request

    case response: RemoteResponse =>
      feedbackRegistry.get(response.channelId)
        .foreach {
          feedback =>

            if (feedback.promise.tryComplete(response.message))
              loggerStats.info(
                s"Execution completed after ${Duration.between(feedback.timestamp, Instant.now).toMillis} ms.")
        }
  }
}

object WorkerFrontend {

  def props(config: BundleConfig,
            logger: Logger,
            loggerStats: Logger,
            backendLocal: ActorRef,
            feedbackRegistry: FeedbackRegistry) =
    Props(classOf[WorkerFrontend], config, logger, loggerStats, backendLocal, feedbackRegistry)
}
