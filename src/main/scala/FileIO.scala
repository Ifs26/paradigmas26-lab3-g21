import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   *         returns empty list if file not found
   */
  def readSubscriptions(filePath: String): List[Option[Subscription]] = {
    implicit val formats: Formats = DefaultFormats

    try {
      val source = Source.fromFile(filePath)
      try {
        val content = source.mkString
        val json = parse(content)

        json match {
          case JArray(items) =>
            items.map { item =>
              val nameOpt = (item \ "name").extractOpt[String]
              val urlOpt = (item \ "url").extractOpt[String]
              (nameOpt, urlOpt) match {
                case (Some(name), Some(url)) => Some(Subscription(name, url))
                case _ =>
                  println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
                  None
              }
            }
          case _ =>
            println(s"Error: Could not load $filePath - invalid JSON format")
            Nil
        }
      } finally {
        source.close()
      }
    } catch {
      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load $filePath - file not found")
        Nil
      case _: org.json4s.ParserUtil.ParseException =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        Nil
      case _: MappingException =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        Nil
      case _: Exception =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        Nil
    }
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    val source = Source.fromURL(url)
    val content = source.mkString
    source.close()
    Some(content)
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    val source = Source.fromFile(filePath)
    val lines = source.getLines()
      .map(_.trim)
      .filter(_.nonEmpty)
      .filterNot(_.startsWith("#"))
      .toList
    source.close()
    Some(lines)
  }
}
