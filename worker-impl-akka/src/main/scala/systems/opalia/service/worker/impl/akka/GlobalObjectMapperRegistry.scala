package systems.opalia.service.worker.impl.akka

import scala.collection.concurrent.TrieMap
import systems.opalia.interfaces.worker.ObjectMapper


object GlobalObjectMapperRegistry {

  private val ObjectMapperMap = TrieMap[Int, ObjectMapper]()

  def add(mapper: ObjectMapper): Unit =
    synchronized {

      if (ObjectMapperMap.contains(mapper.id))
        throw new IllegalArgumentException(s"Mapper with id ${mapper.id} is already registered.")

      ObjectMapperMap += mapper.id -> mapper
    }

  def remove(id: Int): Unit =
    synchronized {

      ObjectMapperMap -= id
    }

  def findByObject(obj: AnyRef): ObjectMapper = {

    ObjectMapperMap
      .find(_._2.canHandle(obj))
      .map(_._2)
      .getOrElse(throw new IllegalArgumentException(s"No mapper available for type ${obj.getClass.getName}."))
  }

  def findById(id: Int): ObjectMapper = {

    ObjectMapperMap.getOrElse(id, throw new IllegalArgumentException(s"No mapper available with id $id."))
  }

  def clear(): Unit =
    synchronized {

      ObjectMapperMap.clear()
    }
}
