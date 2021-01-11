package com.cloudera.spark

import com.cloudera.spark.CGroupsMemoryHandle.{countMetrics, metrics, getMetricsSnapshot}

import scala.io.Source

class CGroupsMemoryHandle extends MemoryGetter {

  override val namesAndReporting = metrics.map { m =>
    val peakReporting = if (countMetrics(m)) IncrementCounts else IncrementBytes
    (m, peakReporting)
  }

  override def values(dest: Array[Long], offset: Int): Unit = {
    var idx = offset
    val metricsSnapshot = getMetricsSnapshot
    metrics.foreach { m =>
      metricsSnapshot.get(m).foreach { newValue =>
        dest(idx) = newValue
      }
      idx += 1
    }
  }
}

object CGroupsMemoryHandle {

  val cGroupMemoryStatPath = "/sys/fs/cgroup/memory/memory.stat"
  // For more information about the metrics please visit
  // https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-memory
  // https://docs.docker.com/config/containers/runmetrics/
  val metrics = Seq("cache", "rss", "mapped_file", "pgpgin", "pgpgout", "swap", "active_anon", "inactive_anon",
      "active_file", "inactive_file", "unevictable", "hierarchical_memory_limit", "hierarchical_memsw_limit")
      .flatMap { metric => Seq(metric, "total_" + metric)}

  val countMetrics = Set("pgpgin", "pgpgout", "total_pgpgin", "total_pgpgout")

  def get(): Option[CGroupsMemoryHandle] = {
    if (new java.io.File(cGroupMemoryStatPath).exists) {
      Some(new CGroupsMemoryHandle())
      } else {
        None
      }
    }

  private def getMetricsSnapshot: Map[String, Long] = {
    val memMappings = Source.fromFile(cGroupMemoryStatPath).getLines
      .flatMap { case line =>
        try {
          val words = line.split(" ")
          Some(words(0) -> words(1).toLong)
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        }
      }.toMap

    metrics.flatMap { metric =>
      memMappings.get(metric) match {
        case Some(value) => Seq((metric, value))
        case None => Seq()
      }
    }.toMap
  }
}

