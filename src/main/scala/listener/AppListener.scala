package listener

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/**
  * Created by Kerven on 2019/4/2.
  */
class AppListener extends SparkListener{

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

  }
}
