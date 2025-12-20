package script

import java.io.File
import scala.sys.process.Process

object ScriptRunner {
  def startKafka(): Unit = {
    val scriptPath = new File("src/main/scala/script/start-kafka.bat")
    val script2 = new File("src/main/scala/script/start-kafka2.bat")

    if (!scriptPath.exists()) {
      throw new RuntimeException("Kafka startup script not found!")
    }

    Process(Seq("cmd", "/c", scriptPath.getAbsolutePath)).run()
    Thread.sleep(2000)
    Process(Seq("cmd", "/c", script2.getAbsolutePath)).run()

    println("Kafka start command issued.")
    Thread.sleep(15000)
  }
}
