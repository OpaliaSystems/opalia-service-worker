package systems.opalia.service.worker.impl.akka

import scala.util.Try
import systems.opalia.commons.identifier.ObjectId
import systems.opalia.interfaces.worker.Message


sealed trait WorkerRecord
  extends Serializable

case class RemoteRequest(channelId: ObjectId,
                         topic: String,
                         message: Message)
  extends WorkerRecord

case class RemoteResponse(channelId: ObjectId,
                          message: Try[Message])
  extends WorkerRecord
