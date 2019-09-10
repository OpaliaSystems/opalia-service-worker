package systems.opalia.service.worker.testing

import com.typesafe.config.Config
import java.util.concurrent.TimeoutException
import play.api.libs.json._
import systems.opalia.bootloader.ArtifactNameBuilder._
import systems.opalia.bootloader.BootloaderBuilder
import systems.opalia.interfaces.rendering.Renderer
import systems.opalia.interfaces.soa.ClientFaultException
import systems.opalia.interfaces.worker._
import systems.opalia.service.worker.testing.helpers.AbstractTest


class WorkerServiceTest
  extends AbstractTest {

  var workerService: WorkerService = _

  def testName: String =
    "worker-service-test"

  def configure(bootBuilder: BootloaderBuilder): BootloaderBuilder = {

    bootBuilder
      .withBundle("systems.opalia" %% "logging-impl-logback" % "1.0.0")
      .withBundle("systems.opalia" %% "worker-impl-akka" % "1.0.0")
  }

  def init(config: Config): Unit = {

    workerService = serviceManager.getService(bundleContext, classOf[WorkerService])
  }

  it should "be able to register object mappers" in {

    workerService.registerObjectMapper(new ObjectMapper {

      def id: Int = 42

      def canHandle(obj: AnyRef): Boolean = {

        obj.isInstanceOf[String]
      }

      def toBinary(obj: AnyRef): Vector[Byte] = {

        obj.asInstanceOf[String].getBytes(Renderer.appDefaultCharset).toVector
      }

      def fromBinary(bytes: Seq[Byte]): AnyRef = {

        new String(bytes.toArray, Renderer.appDefaultCharset)
      }
    })

    workerService.registerObjectMapper(new ObjectMapper {

      def id: Int = 73

      def canHandle(obj: AnyRef): Boolean = {

        obj.isInstanceOf[JsValue]
      }

      def toBinary(obj: AnyRef): Vector[Byte] = {

        Json.toBytes(obj.asInstanceOf[JsValue]).toVector
      }

      def fromBinary(bytes: Seq[Byte]): AnyRef = {

        Json.parse(bytes.toArray)
      }
    })
  }

  it should "be able to deliver messages correctly" in {

    def echo(message: Message): Message =
      message

    val consumer = workerService.newConsumer("my topic", echo)
    val consumer2 = workerService.newConsumer("my topic", echo)
    val producerLocal = workerService.newProducer("my topic", local = true)
    val producerRemote = workerService.newProducer("my topic", local = false)

    consumer.register()

    intercept[IllegalArgumentException](consumer2.register()).getMessage shouldBe
      "Consumer with topic “my topic” is already registered."

    val string = "This is a unicode string (❤ ☀ ☆ ☂ ☻ ♞ ☯ ☭ ☢ €)."

    val json: JsValue =
      Json.parse(
        """
          |{
          |  "name": "Dresden",
          |  "location": {
          |      "lat": 51.049259,
          |      "long": 13.73836
          |  },
          |  "country": "Germany",
          |  "area": 328.48,
          |  "population": 551.072
          |}
        """.stripMargin)

    producerLocal.syncCall(Message("")).value shouldBe ""
    producerLocal.syncCall(Message(string)).value shouldBe string
    producerLocal.syncCall(Message(json)).value shouldBe json
    producerLocal.syncCall(Message("the key", json)).key shouldBe Some("the key")

    producerRemote.syncCall(Message("")).value shouldBe ""
    producerRemote.syncCall(Message(string)).value shouldBe string
    producerRemote.syncCall(Message(json)).value shouldBe json
    producerRemote.syncCall(Message("the key", json)).key shouldBe Some("the key")

    consumer.unregister()
  }

  it should "be able to tunnel errors from consuming function" in {

    val errorMessage = "Oops something went wrong."

    def exception(message: Message): Message =
      throw new ClientFaultException(errorMessage)

    val consumer = workerService.newConsumer("my topic", exception)
    val producerLocal = workerService.newProducer("my topic", local = true)
    val producerRemote = workerService.newProducer("my topic", local = false)

    consumer.register()

    intercept[Exception](producerLocal.syncCall(Message(""))).getMessage shouldBe errorMessage
    intercept[Exception](producerRemote.syncCall(Message(""))).getMessage shouldBe errorMessage

    consumer.unregister()
  }

  it should "notify in case of timeout" in {

    val errorMessage = "Execution timeout after 4000 ms."

    def timeout(message: Message): Message = {

      Thread.sleep(Int.MaxValue)
      null
    }

    val consumer = workerService.newConsumer("my topic", timeout)
    val producerLocal = workerService.newProducer("my topic", local = true)
    val producerRemote = workerService.newProducer("my topic", local = false)
    val producerNoPeer = workerService.newProducer("topic that does not exist", local = true)

    consumer.register()

    intercept[TimeoutException](producerLocal.syncCall(Message(""))).getMessage shouldBe errorMessage
    intercept[TimeoutException](producerRemote.syncCall(Message(""))).getMessage shouldBe errorMessage
    intercept[TimeoutException](producerNoPeer.syncCall(Message(""))).getMessage shouldBe errorMessage

    consumer.unregister()
  }

  it should "be able to unregister object mappers" in {

    workerService.unregisterObjectMapper(42)
    workerService.unregisterObjectMapper(73)
  }
}
