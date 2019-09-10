package systems.opalia.service.worker.impl.akka

import akka.osgi.BundleDelegatingClassLoader
import org.osgi.framework.BundleContext
import org.osgi.service.component.annotations._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import systems.opalia.interfaces.logging.LoggingService
import systems.opalia.interfaces.soa.ConfigurationService
import systems.opalia.interfaces.soa.osgi.ServiceManager
import systems.opalia.interfaces.worker._


@Component(service = Array(classOf[WorkerService]), immediate = true)
class WorkerServiceImpl
  extends WorkerService {

  private val serviceManager: ServiceManager = new ServiceManager()
  private var bootable: WorkerServiceBootable = _

  @Reference
  private var loggingService: LoggingService = _

  @Activate
  def start(bundleContext: BundleContext): Unit = {

    val classLoader =
      BundleDelegatingClassLoader(bundleContext, Some(Thread.currentThread().getContextClassLoader))

    val configurationService = serviceManager.getService(bundleContext, classOf[ConfigurationService])
    val config = new BundleConfig(configurationService.getConfiguration)

    bootable =
      new WorkerServiceBootable(
        config,
        classLoader,
        loggingService,
      )

    bootable.setup()
    Await.result(bootable.awaitUp(), Duration.Inf)
  }

  @Deactivate
  def stop(bundleContext: BundleContext): Unit = {

    bootable.shutdown()
    Await.result(bootable.awaitUp(), Duration.Inf)

    serviceManager.ungetServices(bundleContext)

    bootable = null
  }

  def newProducer(topic: String, local: Boolean): Producer =
    bootable.newProducer(topic, local)

  def newConsumer(topic: String, consume: (Message) => Message): Consumer =
    bootable.newConsumer(topic, consume)

  def registerObjectMapper(objectMapper: ObjectMapper): Unit =
    bootable.registerObjectMapper(objectMapper)

  def unregisterObjectMapper(id: Int): Unit =
    bootable.unregisterObjectMapper(id)
}
