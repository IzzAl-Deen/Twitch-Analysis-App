import producer.ProducerCollect
import script.ScriptRunner

object ProducerRunnable extends App {
  //ScriptRunner.startKafka()
  ProducerCollect.RunProducer()
}
