package listener

import org.apache.spark.streaming.scheduler._

/**
  * Created by Kerven on 2019/4/11.
  */
class MyStreamingListener extends StreamingListener{
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = super.onReceiverStarted(receiverStarted)

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = super.onReceiverError(receiverError)

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = super.onReceiverStopped(receiverStopped)

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = super.onBatchSubmitted(batchSubmitted)

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = super.onBatchStarted(batchStarted)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
//    batchCompleted.batchInfo.
  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = super.onOutputOperationStarted(outputOperationStarted)

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = super.onOutputOperationCompleted(outputOperationCompleted)
}
