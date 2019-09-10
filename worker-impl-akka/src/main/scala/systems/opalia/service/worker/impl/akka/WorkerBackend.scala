package systems.opalia.service.worker.impl.akka

import akka.actor._
import scala.concurrent.Future
import scala.util.Failure
import systems.opalia.commons.identifier.ObjectId
import systems.opalia.interfaces.logging.Logger
import systems.opalia.interfaces.soa.{ClientFaultException, ServiceFaultException}
import systems.opalia.interfaces.worker.Message


final class WorkerBackend(config: BundleConfig,
                          logger: Logger,
                          loggerStats: Logger,
                          consumerRegistry: ConsumerRegistry)
  extends Actor {

  private implicit val executionContext = context.dispatcher

  def receive = {

    case RemoteRequest(channelId, topic, message) =>
      channel(
        consumerRegistry.findAndApply(topic, message),
        channelId,
        sender()
      )
  }

  private def channel(future: Future[Message], channelId: ObjectId, recipient: ActorRef): Unit = {

    future
      .onComplete {
        result =>

          result match {
            case Failure(e: ClientFaultException) =>
              logger.debug("A client error occurred during consumption.", e)
            case Failure(e: ServiceFaultException) =>
              logger.error("A service error occurred during consumption.", e)
            case Failure(e) =>
              logger.error("An error occurred during consumption.", e)
            case _ =>
          }

          recipient ! RemoteResponse(channelId, result)
      }
  }
}

object WorkerBackend {

  def props(config: BundleConfig,
            logger: Logger,
            loggerStats: Logger,
            consumerRegistry: ConsumerRegistry) =
    Props(classOf[WorkerBackend], config, logger, loggerStats, consumerRegistry)
}
