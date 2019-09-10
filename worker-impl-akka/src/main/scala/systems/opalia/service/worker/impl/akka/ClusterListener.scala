package systems.opalia.service.worker.impl.akka

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.metrics._
import systems.opalia.interfaces.logging.Logger


final class ClusterListener(config: BundleConfig,
                            logger: Logger,
                            loggerStats: Logger)
  extends Actor {

  val cluster = Cluster(context.system)
  val extension = ClusterMetricsExtension(context.system)

  override def preStart(): Unit = {

    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    extension.subscribe(self)
  }

  override def postStop(): Unit = {

    cluster.unsubscribe(self)
    extension.unsubscribe(self)
  }

  def receive = {

    case state: CurrentClusterState =>
      logger.info(s"Current members are ${state.members.map(_.address).mkString(", ")}.")

    case MemberUp(member) =>
      logger.info(s"Member ${member.address} is ready.")

    case UnreachableMember(member) =>
      logger.info(s"Member ${member.address} is detected as unreachable.")

    case MemberRemoved(member, previousStatus) =>
      logger.info(s"Member ${member.address} with previous status $previousStatus is removed.")

    case ClusterMetricsChanged(clusterMetrics) =>
      clusterMetrics.foreach(logMetrics)
  }

  def logMetrics(nodeMetrics: NodeMetrics): Unit = {

    if (loggerStats.infoEnabled) {

      val metrics =
        Map(
          "address" ->
            Some(nodeMetrics.address),
          "heap_memory_used" ->
            nodeMetrics.metric(StandardMetrics.HeapMemoryUsed).map(_.smoothValue.longValue),
          "heap_memory_committed" ->
            nodeMetrics.metric(StandardMetrics.HeapMemoryCommitted).map(_.smoothValue.longValue),
          "heap_memory_max" ->
            nodeMetrics.metric(StandardMetrics.HeapMemoryMax).map(_.smoothValue.longValue),
          "processors" ->
            nodeMetrics.metric(StandardMetrics.Processors).map(_.value.intValue),
          "system_load_average" ->
            nodeMetrics.metric(StandardMetrics.SystemLoadAverage).map(_.smoothValue),
          "cpu_combined" ->
            nodeMetrics.metric(StandardMetrics.CpuCombined).map(_.smoothValue),
          "cpu_stolen" ->
            nodeMetrics.metric(StandardMetrics.CpuStolen).map(_.smoothValue),
          "cpu_idle" ->
            nodeMetrics.metric(StandardMetrics.CpuIdle).map(_.smoothValue)
        )

      metrics.foreach {
        case (k, v) =>

          val value = v.map(_.toString).getOrElse("NONE")

          loggerStats.info(s"metrics: ${nodeMetrics.timestamp} - ${cluster.selfAddress} - $k: ${value}")
      }
    }
  }
}

object ClusterListener {

  def props(config: BundleConfig,
            logger: Logger,
            loggerStats: Logger) =
    Props(classOf[ClusterListener], config, logger, loggerStats)
}
