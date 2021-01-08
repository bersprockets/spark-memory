package com.cloudera.spark

import com.cloudera.spark.CGroupsMemoryHandle.{cGroupMemoryStatPath, getCGroupMemStats}

import scala.io.Source

class CGroupsMemoryHandle(cgroupMemStats: Seq[(String, Long)]) extends MemoryGetter {

  override val namesAndReporting: Seq[(String, PeakReporting)] = cgroupMemStats.map(x =>
    if (Set("pgpgin", "pgpgout", "total_pgpgin", "total_pgpgout")(x._1)) {
      (x._1, IncrementCounts)
    } else {
      (x._1, IncrementBytes)
    }
  )

  override def values(dest: Array[Long], offset: Int): Unit = {
    if (new java.io.File(cGroupMemoryStatPath).exists) {
      var counter = 0
      getCGroupMemStats.foreach{ case (_, metricValue) =>
        dest(offset + counter) = metricValue
        counter += 1
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

  private def getCGroupMemStats = {
    Source.fromFile(cGroupMemoryStatPath).getLines
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
      }.toSeq
  }
}

