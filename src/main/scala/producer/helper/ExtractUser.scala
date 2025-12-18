package producer.helper

object ExtractUser {
    def extractUser (line : String): String = {
        val name = "display-name=" //the display-name= is from the API

        if(line.contains(name)){
          val start = line.indexOf(name) + name.length
          val end = line.indexOf(";",start)

          line.substring(start,end)
        }
        else {
          "unknown User"
        }

    }
}
