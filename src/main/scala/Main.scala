import producer.ProducerCollect
import script.ScriptRunner

object Main extends App {

  ScriptRunner.startKafka()
  ProducerCollect.RunProducer()



}
