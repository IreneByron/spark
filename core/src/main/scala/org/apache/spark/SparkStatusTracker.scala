/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.util.Arrays

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.util.Utils

/**
 * Low-level status reporting APIs for monitoring job and stage progress.
 * 用于监视作业和阶段进度的低级状态报告API。
 *
 * These APIs intentionally provide very weak consistency semantics; consumers of these APIs should
 * be prepared to handle empty / missing information.  For example, a job's stage ids may be known
 * but the status API may not have any information about the details of those stages, so
 * `getStageInfo` could potentially return `None` for a valid stage id.
 * 这些API有意提供非常弱的一致性语义。
 * 这些API的使用者应准备处理空的/丢失的信息。
 * 例如，一个工作的阶段ID可能是已知的，
 * 但是状态API可能没有关于那些阶段的详细信息的任何信息，
 * 因此getStageInfo可能会为有效的阶段ID返回None。
 *
 * To limit memory usage, these APIs only provide information on recent jobs / stages.  These APIs
 * will provide information for the last `spark.ui.retainedStages` stages and
 * `spark.ui.retainedJobs` jobs.
 * 为了限制内存使用，这些API仅提供有关最近作业/阶段的信息。
 * 这些API将为最后的“ spark.ui.retainedStages”阶段和“ spark.ui.retainedJobs”作业提供信息。
 *
 * NOTE: this class's constructor should be considered private and may be subject to change.
 */
class SparkStatusTracker private[spark] (sc: SparkContext, store: AppStatusStore) {

  /**
   * Return a list of all known jobs in a particular job group.  If `jobGroup` is `null`, then
   * returns all known jobs that are not associated with a job group.
   * 返回特定作业组中所有已知作业的列表。如果“ jobGroup”为“ null”，则返回所有与工作组无关的已知工作。
   *
   * The returned list may contain running, failed, and completed jobs, and may vary across
   * invocations of this method.  This method does not guarantee the order of the elements in
   * its result.
   * 返回的列表可能包含正在运行的，失败的和已完成的作业，
   * 并且在此方法的调用之间可能会有所不同。此方法不能保证结果中元素的顺序。
   */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = {
    val expected = Option(jobGroup)
    store.jobsList(null).filter(_.jobGroup == expected).map(_.jobId).toArray
  }

  /**
   * Returns an array containing the ids of all active stages.
   *
   * This method does not guarantee the order of the elements in its result.
   */
  def getActiveStageIds(): Array[Int] = {
    store.stageList(Arrays.asList(StageStatus.ACTIVE)).map(_.stageId).toArray
  }

  /**
   * Returns an array containing the ids of all active jobs.
   *
   * This method does not guarantee the order of the elements in its result.
   */
  def getActiveJobIds(): Array[Int] = {
    store.jobsList(Arrays.asList(JobExecutionStatus.RUNNING)).map(_.jobId).toArray
  }

  /**
   * Returns job information, or `None` if the job info could not be found or was garbage collected.
   */
  def getJobInfo(jobId: Int): Option[SparkJobInfo] = {
    store.asOption(store.job(jobId)).map { job =>
      new SparkJobInfoImpl(jobId, job.stageIds.toArray, job.status)
    }
  }

  /**
   * Returns stage information, or `None` if the stage info could not be found or was
   * garbage collected.
   */
  def getStageInfo(stageId: Int): Option[SparkStageInfo] = {
    store.asOption(store.lastStageAttempt(stageId)).map { stage =>
      new SparkStageInfoImpl(
        stageId,
        stage.attemptId,
        stage.submissionTime.map(_.getTime()).getOrElse(0L),
        stage.name,
        stage.numTasks,
        stage.numActiveTasks,
        stage.numCompleteTasks,
        stage.numFailedTasks)
    }
  }

  /**
   * Returns information of all known executors, including host, port, cacheSize, numRunningTasks
   * and memory metrics.
   * Note this include information for both the driver and executors.
   */
  def getExecutorInfos: Array[SparkExecutorInfo] = {
    store.executorList(true).map { exec =>
      val (host, port) = Utils.parseHostPort(exec.hostPort)
      val cachedMem = exec.memoryMetrics.map { mem =>
        mem.usedOnHeapStorageMemory + mem.usedOffHeapStorageMemory
      }.getOrElse(0L)

      new SparkExecutorInfoImpl(
        host,
        port,
        cachedMem,
        exec.activeTasks,
        exec.memoryMetrics.map(_.usedOffHeapStorageMemory).getOrElse(0L),
        exec.memoryMetrics.map(_.usedOnHeapStorageMemory).getOrElse(0L),
        exec.memoryMetrics.map(_.totalOffHeapStorageMemory).getOrElse(0L),
        exec.memoryMetrics.map(_.totalOnHeapStorageMemory).getOrElse(0L))
    }.toArray
  }
}
