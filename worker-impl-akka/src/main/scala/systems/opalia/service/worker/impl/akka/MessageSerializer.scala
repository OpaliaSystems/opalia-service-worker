package systems.opalia.service.worker.impl.akka

import akka.serialization.SerializerWithStringManifest
import java.nio.ByteBuffer
import scala.util.{Failure, Success}
import systems.opalia.commons.identifier.ObjectId
import systems.opalia.interfaces.rendering.Renderer
import systems.opalia.interfaces.worker.{Message, ObjectMapper}


final class MessageSerializer
  extends SerializerWithStringManifest {

  private val manifestRemoteRequest = "remote_request"
  private val manifestRemoteResponseSuccess = "remote_response_success"
  private val manifestRemoteResponseFailure = "remote_response_failure"

  private val escapeByte = 0xFF.toByte

  def identifier: Int =
    1928374650

  def manifest(obj: AnyRef): String =
    obj match {

      case RemoteRequest(_, _, _) =>
        manifestRemoteRequest

      case RemoteResponse(_, Success(_)) =>
        manifestRemoteResponseSuccess

      case RemoteResponse(_, Failure(_)) =>
        manifestRemoteResponseFailure
    }

  def toBinary(obj: AnyRef): Array[Byte] = {

    obj match {

      case RemoteRequest(channelId, topic, message) => {

        val keyMapper = message.key.map(x => findMapper(x))
        val valueMapper = findMapper(message.value)

        val bytes =
          escape(channelId) ++
            List(escapeByte) ++
            escape(convString2Bytes(topic)) ++
            List(escapeByte) ++
            keyMapper.map(x => escape(convInt2Bytes(x.id))).getOrElse(Nil) ++
            List(escapeByte) ++
            escape(convInt2Bytes(valueMapper.id)) ++
            List(escapeByte) ++
            escape(convLong2Bytes(message.timestamp)) ++
            List(escapeByte) ++
            keyMapper.map(x => escape(x.toBinary(message.key.get))).getOrElse(Nil) ++
            List(escapeByte) ++
            valueMapper.toBinary(message.value)

        bytes.toArray
      }

      case RemoteResponse(channelId, Success(message)) => {

        val keyMapper = message.key.map(x => findMapper(x))
        val valueMapper = findMapper(message.value)

        val bytes =
          escape(channelId) ++
            List(escapeByte) ++
            keyMapper.map(x => escape(convInt2Bytes(x.id))).getOrElse(Nil) ++
            List(escapeByte) ++
            escape(convInt2Bytes(valueMapper.id)) ++
            List(escapeByte) ++
            escape(convLong2Bytes(message.timestamp)) ++
            List(escapeByte) ++
            keyMapper.map(x => escape(x.toBinary(message.key.get))).getOrElse(Nil) ++
            List(escapeByte) ++
            valueMapper.toBinary(message.value)

        bytes.toArray
      }

      case RemoteResponse(channelId, Failure(error)) => {

        val bytes =
          escape(channelId) ++
            List(escapeByte) ++
            stdSerialize(error)

        bytes.toArray
      }
    }
  }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {

    manifest match {

      case `manifestRemoteRequest` => {

        val data = extract(bytes, 7)

        val channelId = ObjectId.getFrom(unescape(data(0)))
        val topic = convBytes2String(unescape(data(1)))
        val keyMapper = optional(data(2)).map(x => findMapper(convBytes2Int(unescape(x))))
        val valueMapper = findMapper(convBytes2Int(unescape(data(3))))
        val _timestamp = convBytes2Long(unescape(data(4)))
        lazy val _key = keyMapper.map(x => x.fromBinary(unescape(data(5))))
        lazy val _value = valueMapper.fromBinary(data(6))

        val message =
          new Message {

            def timestamp: Long =
              _timestamp

            def key: Option[AnyRef] =
              _key

            def value: AnyRef =
              _value
          }

        RemoteRequest(channelId, topic, message)
      }

      case `manifestRemoteResponseSuccess` => {

        val data = extract(bytes, 6)

        val channelId = ObjectId.getFrom(unescape(data(0)))
        val keyMapper = optional(data(1)).map(x => findMapper(convBytes2Int(unescape(x))))
        val valueMapper = findMapper(convBytes2Int(unescape(data(2))))
        val _timestamp = convBytes2Long(unescape(data(3)))
        lazy val _key = keyMapper.map(x => x.fromBinary(unescape(data(4))))
        lazy val _value = valueMapper.fromBinary(data(5))

        val message =
          new Message {

            def timestamp: Long =
              _timestamp

            def key: Option[AnyRef] =
              _key

            def value: AnyRef =
              _value
          }

        RemoteResponse(channelId, Success(message))
      }

      case `manifestRemoteResponseFailure` => {

        val data = extract(bytes, 2)

        val channelId = ObjectId.getFrom(unescape(data(0)))
        val error = stdDeserialize[Throwable](data(1).toArray)

        RemoteResponse(channelId, Failure(error))
      }
    }
  }

  private def extract(bytes: Array[Byte], length: Int): Vector[Seq[Byte]] = {

    def process(start: Int, length: Int): Vector[Seq[Byte]] = {

      if (length > 1) {

        val end = bytes.indexOf(escapeByte, start)

        if (end == -1)
          throw new IllegalArgumentException("Cannot create object from bytes.")

        bytes.slice(start, end).toSeq +: process(end + 1, length - 1)

      } else
        Vector(bytes.slice(start, bytes.length))
    }

    process(0, length)
  }

  private def escape(bytes: Seq[Byte]): Seq[Byte] = {

    val b1 = 0x0F.toByte
    val b2 = 0x0E.toByte

    def process(list: List[Byte]): List[Byte] =
      list match {
        case x :: xs if (x == b1) => b1 :: b1 :: process(xs)
        case x :: xs if (x == escapeByte) => b1 :: b2 :: process(xs)
        case x :: xs => x :: process(xs)
        case Nil => Nil
      }

    process(bytes.toList)
  }

  private def unescape(bytes: Seq[Byte]): Seq[Byte] = {

    val b1 = 0x0F.toByte
    val b2 = 0x0E.toByte

    def process(list: List[Byte]): List[Byte] =
      list match {
        case `b1` :: `b1` :: xs => b1 :: process(xs)
        case `b1` :: `b2` :: xs => escapeByte :: process(xs)
        case x :: xs => x :: process(xs)
        case Nil => Nil
      }

    process(bytes.toList)
  }

  private def findMapper(identifier: Int): ObjectMapper =
    GlobalObjectMapperRegistry.findById(identifier)

  private def findMapper(obj: AnyRef): ObjectMapper =
    GlobalObjectMapperRegistry.findByObject(obj)

  private def optional(bytes: Seq[Byte]): Option[Seq[Byte]] = {

    if (bytes.nonEmpty)
      Some(bytes)
    else
      None
  }

  private def convInt2Bytes(value: Int): Seq[Byte] = {

    ByteBuffer.allocate(4)
      .order(Renderer.appDefaultByteOrder)
      .putInt(value)
      .array
  }

  private def convLong2Bytes(value: Long): Seq[Byte] = {

    ByteBuffer.allocate(8)
      .order(Renderer.appDefaultByteOrder)
      .putLong(value)
      .array
  }

  private def convBytes2Int(bytes: Seq[Byte]): Int = {

    if (bytes.length != java.lang.Integer.BYTES)
      throw new IllegalArgumentException("Unexpected length of byte sequence for 4 byte integer.")

    ByteBuffer.wrap(bytes.toArray)
      .order(Renderer.appDefaultByteOrder)
      .getInt
  }

  private def convBytes2Long(bytes: Seq[Byte]): Long = {

    if (bytes.length != java.lang.Long.BYTES)
      throw new IllegalArgumentException("Unexpected length of byte sequence for 8 byte integer.")

    ByteBuffer.wrap(bytes.toArray)
      .order(Renderer.appDefaultByteOrder)
      .getLong
  }

  private def convString2Bytes(string: String): Seq[Byte] = {

    string.getBytes(Renderer.appDefaultCharset)
  }

  private def convBytes2String(bytes: Seq[Byte]): String = {

    new String(bytes.toArray, Renderer.appDefaultCharset)
  }

  private def stdSerialize[T](obj: T): Array[Byte] = {

    import java.io.{ByteArrayOutputStream, ObjectOutputStream}

    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)

    objectStream.writeObject(obj)
    objectStream.flush()

    byteStream.toByteArray
  }

  private def stdDeserialize[T](bytes: Array[Byte]): T = {

    import java.io.{ByteArrayInputStream, ObjectInputStream}

    val byteStream = new ByteArrayInputStream(bytes)
    val objectStream = new ObjectInputStream(byteStream)

    objectStream.readObject().asInstanceOf[T]
  }
}
