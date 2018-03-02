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
package org.apache.spark.ml.spark

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.storage.RDDInfo
import water.Job

import scala.collection.mutable

/**
  * One progress listener for each instance of model creation. Each listener gets all events so this particular one
  * will respond only to the ones corresponding to a given RDD used for training (which should not be shared) and
  * to a particular job type (depending on the algorithm).
  *
  * It might not be 100% accurate depending on how many "non primary" jobs will be run.
  *
  * @param job        Job which progress is to be updated
  * @param rddInfo    events triggered for this RDD (or its children) trigger progress change
  * @param stageNames which stages are supposed to change the progress bar %
  */
class ProgressListener(val sc: SparkContext,
                       val job: Job[_],
                       val rddInfo: RDDInfo,
                       val stageNames: Iterable[String]) extends SparkListener {

  var currentJobs: scala.collection.mutable.Map[Int, Int] = mutable.HashMap()

  private def matchingRDD(stageInfo: StageInfo): Boolean = stageInfo.rddInfos.map(_.id).contains(rddInfo.id)

  private def matchingRDD(stageInfos: Seq[StageInfo]): Boolean = stageInfos.exists(matchingRDD)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (matchingRDD(jobStart.stageInfos)) {
      job.update(0, s"Staring Spark job with ID [${jobStart.jobId}]")

      if (jobStart.stageInfos.exists(stage => stageNames.exists(stage.name.contains))) {
        currentJobs.put(jobStart.jobId, 1)
      } else {
        currentJobs.put(jobStart.jobId, 0)
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (currentJobs.contains(jobEnd.jobId)) {
      job.update(currentJobs(jobEnd.jobId), s"Finished Spark job with ID [${jobEnd.jobId}]")
      currentJobs.remove(jobEnd.jobId)
    }
  }

  var currentStages: scala.collection.mutable.Map[Int, String] = mutable.HashMap()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (matchingRDD(stageSubmitted.stageInfo)) {
      updateTaskStatus(stageSubmitted.stageInfo.stageId, 0)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (currentStages.contains(stageCompleted.stageInfo.stageId)) {
      currentStages.remove(stageCompleted.stageInfo.stageId)
      job.update(0, printStagesStatus())
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    if (currentStages.contains(taskStart.stageId)) {
      updateTaskStatus(taskStart.stageId, taskStart.taskInfo.index + 1)
    }
  }

  private def updateTaskStatus(stageId: Int, taskIdx: Int): Unit = {
    val status = s"Stage [$stageId] status [$taskIdx/${sc.statusTracker.getStageInfo(stageId).get.numTasks}]."
    currentStages.put(stageId, status)
    job.update(0, printStagesStatus())
  }

  private def printStagesStatus(): String = currentStages.map { case (id, status) => status }.mkString("\n")


}
