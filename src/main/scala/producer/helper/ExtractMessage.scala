package producer.helper

object ExtractMessage {

  def extractMessage(line: String): String = { //from the API
    val idx = line.indexOf("PRIVMSG")
    if (idx != -1) {
      val lined = line.substring(line.indexOf(" :", idx) + 2) //to get the message after the ( :)
      lined
    } else {
      ""
    }
  }
}
