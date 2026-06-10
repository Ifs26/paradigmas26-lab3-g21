object Formatters {

  /**
   * Format processing statistics for console output.
   * @param stats map with keys: "feedsSuccess", "feedsFailed", "postsSuccess", "postsFailed", "postsFiltered", "avgChars"
   * @return formatted statistics string
   */
  def formatProcessingStats(stats: Map[String, Int]): String = {
    val feedsSuccess = stats.getOrElse("feedsSuccess", 0)
    val feedsFailed = stats.getOrElse("feedsFailed", 0)
    val postsSuccess = stats.getOrElse("postsSuccess", 0)
    val postsFailed = stats.getOrElse("postsFailed", 0)
    val postsFiltered = stats.getOrElse("postsFiltered", 0)
    val avgChars = stats.getOrElse("avgChars", 0)

    s"""============ ESTADÍSTICAS DE PROCESAMIENTO ============
Feeds descargados exitosamente: $feedsSuccess
Feeds fallidos: $feedsFailed
Posts descargados exitosamente: $postsSuccess
Posts fallidos: $postsFailed
Posts filtrados (vacíos/nulos): $postsFiltered
Largo promedio en posts: $avgChars"""
  }

  /**
   * DESPRECIADA
   * Format entity type statistics for console output.
   * @param typeStats map from entityType to count (including "total" key)
   * @return formatted entity statistics string
   */
  def formatTypeStats(typeStats: Map[String, Int]): String = {
    val total = typeStats.getOrElse("total", 0)
    val entityTypes = List("Person", "Organization", "University", "Place", "Technology", "ProgrammingLanguage")
    val typeLines = entityTypes.map { entityType =>
      val count = typeStats.getOrElse(entityType, 0)
      s"    [$entityType]: $count"
    }.mkString("\n")

    s"""============ ESTADÍSTICAS DE ENTIDADES ============
Entidades totales: $total
Entidades por categoría:
$typeLines"""
  }

  /**
   * Format entity statistics for console output.
   * Sorts by count (descending), then type (alphabetical), then name (alphabetical).
   * @param entityCounts map from (entityType, entityName) to count
   * @param topK number of top entities to display (default: 10)
   * @return formatted entity statistics string
   */
  def formatEntityStats(entityCounts: Map[(String, String), Int], topK: Int = 10): String = {
    val sorted = entityCounts
      .toList
      .sortBy { case ((entityType, entityName), count) =>
        (-count, entityType, entityName)
      }
      .take(topK)

    val formatted = sorted.map { case ((entityType, entityName), count) =>
      s"[Type=$entityType] $entityName: $count apariciones"
    }.mkString("\n")

    s"""============ ENTIDADES NOMBRADAS MÁS FRECUENTES ============
$formatted"""
  }

  /**
   * Format entity type statistics for console output with data obteined from a distributed system.
   * @param typeStats map from entityType to count (including "total" key)
   * @return formatted entity statistics string
   */
  def formatTypeStatsDistributed(entitiesList: List[((String,String),Int)]): String = {

    val reduced = entitiesList.map{entity => (entity._1._1,entity._2)} //List[tipo,cantidad]
    val grouped = reduced.groupBy(_._1)//Map[tipo,(tipo,cantidad)]
    val totalPerTypeMap = grouped//Map[tipo,cantidad total]
    .map{ 
      case (tipo, xs) => (tipo, xs.map(_._2).sum) // sumar cantidades
    }

    val entityTypes = List("Person", "Organization", "University", "Place", "Technology", "ProgrammingLanguage")
    val typeLines = entityTypes.map { entityType => 
      val count = totalPerTypeMap.get(entityType).getOrElse(0)
      s"    [${entityType}]: ${count}"
    }.mkString("\n")

    val allTotals = totalPerTypeMap.values.sum

    s"""============ ESTADÍSTICAS DE ENTIDADES ============
Entidades totales: $allTotals
Entidades por categoría:
$typeLines"""  
  }
}
