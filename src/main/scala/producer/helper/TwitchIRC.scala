package producer.helper
import java.net.Socket
import java.io._
import config.TwitchConfig


object TwitchIRC {
  val socket = new Socket(TwitchConfig.server, TwitchConfig.port)
  val out = new PrintWriter(socket.getOutputStream, true)
  val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
}
