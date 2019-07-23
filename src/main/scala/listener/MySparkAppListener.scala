package listener

import org.apache.spark.scheduler._

/**
  * Created by Kerven on 2019/4/11.
  */
class MySparkAppListener extends SparkListener{
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = super.onStageCompleted(stageCompleted)

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = super.onStageSubmitted(stageSubmitted)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = super.onTaskStart(taskStart)

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = super.onTaskGettingResult(taskGettingResult)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = super.onTaskEnd(taskEnd)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = super.onJobStart(jobStart)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = super.onJobEnd(jobEnd)

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = super.onEnvironmentUpdate(environmentUpdate)

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = super.onBlockManagerAdded(blockManagerAdded)

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = super.onBlockManagerRemoved(blockManagerRemoved)

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = super.onUnpersistRDD(unpersistRDD)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appName = applicationStart.appName
    println("============name=============" + appName)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val endTime = applicationEnd.time
    println("============time=============" + endTime)
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = super.onExecutorMetricsUpdate(executorMetricsUpdate)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = super.onExecutorAdded(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = super.onExecutorRemoved(executorRemoved)

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = super.onBlockUpdated(blockUpdated)

  override def onOtherEvent(event: SparkListenerEvent): Unit = super.onOtherEvent(event)
}
