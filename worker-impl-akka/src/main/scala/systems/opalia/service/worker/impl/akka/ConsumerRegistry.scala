package systems.opalia.service.worker.impl.akka

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import systems.opalia.interfaces.worker.{Consumer, Message}


final class ConsumerRegistry() {

  private val consumerMap = TrieMap[String, Consumer]()

  def add(consumer: Consumer): Unit =
    synchronized {

      if (consumerMap.contains(consumer.topic))
        throw new IllegalArgumentException(s"Consumer with topic “${consumer.topic}” is already registered.")

      consumerMap += consumer.topic -> consumer
    }

  def remove(topic: String): Unit =
    synchronized {

      if (!consumerMap.contains(topic))
        throw new IllegalArgumentException(s"Consumer with topic “$topic” is not registered.")

      consumerMap -= topic
    }

  def findAndApply(topic: String, message: Message)
                  (implicit executionContext: ExecutionContext): Future[Message] = {

    consumerMap
      .get(topic)
      .map(x => Future(x.apply(message)))
      .getOrElse(throw new IllegalArgumentException(s"No consumer available with topic “$topic”."))
  }

  def clear(): Unit =
    synchronized {

      consumerMap.clear()
    }
}
