package com.cloudera.spark

import com.cloudera.spark.CGroupsMemoryHandle.cGroupMemoryStatPath

import scala.io.Source

class CGroupsMemoryHandle(cgroupMemStats: Seq[(String, Long)]) extends MemoryGetter {

  override val namesAndReporting: Seq[(String, PeakReporting)] = cgroupMemStats.map(x =>
    (x._1, IncrementBytes)
  )

  override def values(dest: Array[Long], offset: Int): Unit = {
    var counter = 0
    Source.fromFile(cGroupMemoryStatPath).getLines
      .map { line =>
        val words = line.split(" ")
        words{0} -> words{1}.toLong
      }.toSeq.foreach{ case (_, metricValue) =>
      dest(offset + counter) = metricValue
      counter += 1
    }
  }
}

object CGroupsMemoryHandle {

  val cGroupMemoryStatPath = "/sys/fs/cgroup/memory/memory.stat"

  def get(): Option[CGroupsMemoryHandle] = {
    if (new java.io.File(cGroupMemoryStatPath).exists) {
      val cgroupMemStats = Source.fromFile(cGroupMemoryStatPath).getLines
        .map { line =>
          val words = line.split(" ")
          words{0} -> words{1}.toLong
        }.toSeq
      Some(new CGroupsMemoryHandle(cgroupMemStats))
      } else {
        None
      }
    }
}

