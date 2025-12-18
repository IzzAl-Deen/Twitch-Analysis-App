package producer.helper

object ExtractTimestamp {
  val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.systemDefault())

  def extractTimestamp(line: String): String = {
    val key = "tmi-sent-ts="
    if (line.startsWith("@") && line.contains(key)) {
      val start = line.indexOf(key) + key.length
      val end = line.indexOf(";", start)
      val ts = line.substring(start, end).toLong
      // Convert Twitch timestamp to human-readable date
      formatter.format(Instant.ofEpochMilli(ts))
    } else {
      "unknown"
    }
  }
}
