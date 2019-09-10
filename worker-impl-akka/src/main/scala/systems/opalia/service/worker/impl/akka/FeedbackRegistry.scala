package systems.opalia.service.worker.impl.akka

import java.time.Instant
import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise
import systems.opalia.commons.identifier.ObjectId
import systems.opalia.interfaces.worker.Message


final class FeedbackRegistry() {

  private val feedbackMap = TrieMap[ObjectId, FeedbackRegistry.Feedback]()

  def waitFor(promise: Promise[Message]): ObjectId = {

    val id = ObjectId.getNew
    val timestamp = Instant.now

    feedbackMap += id -> FeedbackRegistry.Feedback(timestamp, promise)

    id
  }

  def remove(id: ObjectId): Unit = {

    feedbackMap -= id
  }

  def get(id: ObjectId): Option[FeedbackRegistry.Feedback] = {

    feedbackMap.get(id)
  }

  def clear(): Unit =
    synchronized {

      feedbackMap.clear()
    }
}

object FeedbackRegistry {

  case class Feedback(timestamp: Instant, promise: Promise[Message])

}
