package com.cloudera.spark

import com.cloudera.spark.CGroupsMemoryHandle.{cGroupMemoryStatPath, getCGroupMemStats}

import scala.io.Source

class CGroupsMemoryHandle(cgroupMemStats: Map[String, Long]) extends MemoryGetter {

  var orderedNames: Option[Seq[String]] = None

  override val namesAndReporting: Seq[(String, PeakReporting)] = {
    val namesAndReporting = cgroupMemStats.toSeq.map { x =>
      if (Set("pgpgin", "pgpgout", "total_pgpgin", "total_pgpgout")(x._1)) {
        (x._1, IncrementCounts)
      } else {
        (x._1, IncrementBytes)
      }
    }
    orderedNames = Some(namesAndReporting.map { x => x._1})
    namesAndReporting
  }

  override def values(dest: Array[Long], offset: Int): Unit = {
    if (new java.io.File(cGroupMemoryStatPath).exists) {
      var counter = 0
      orderedNames match {
        case Some(names) => {
          names.foreach { name =>
            getCGroupMemStats.get(name) match {
              case Some(metricValue) => {
                dest(offset + counter) = metricValue
              }
            }
            // counter value is being increased irrespective of whether a value is found
            // to ensure that the metrics are read in order. if a metric value is not
            // found for a metric it's value is simply not updated in the dest array.
            counter += 1
          }
        }
      }
    }
  }
}

object CGroupsMemoryHandle {

  val cGroupMemoryStatPath = "/sys/fs/cgroup/memory/memory.stat"

  def get(): Option[CGroupsMemoryHandle] = {
    if (new java.io.File(cGroupMemoryStatPath).exists) {
      val cgroupMemStats = getCGroupMemStats
      Some(new CGroupsMemoryHandle(cgroupMemStats))
      } else {
        None
      }
    }

  private def getCGroupMemStats: Map[String, Long] = {
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

    // For more information about the metrics please visit
    // https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-memory
    // https://docs.docker.com/config/containers/runmetrics/
    Seq("cache", "rss", "mapped_file", "pgpgin", "pgpgout", "swap", "active_anon", "inactive_anon",
    "active_file", "inactive_file", "unevictable", "hierarchical_memory_limit", "hierarchical_memsw_limit")
      .flatMap { metric => Seq(metric, "total_" + metric)}
      .flatMap { metric =>
        memMappings.get(metric) match {
          case Some(value) => Seq((metric, value))
          case None => Seq()
        }
      }.toMap
  }
}

