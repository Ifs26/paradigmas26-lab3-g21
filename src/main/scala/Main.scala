import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    val spark = SparkSession.builder().appName("RedditNER").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }
    val subsRDD = sc.parallelize(subscriptions)

    // Accumulators for monitored statistics
    val accFeedsSuccess = sc.longAccumulator("feedsSuccess")
    val accFeedsFailed = sc.longAccumulator("feedsFailed")
    val accPostsDownloaded = sc.longAccumulator("postsDownloaded")
    val accPostGroupsFailed = sc.longAccumulator("postGroupsFailed")

    // Download feeds and parse posts, tracking success/failure
    val baseUrl = sys.env.getOrElse("REDDIT_BASE_URL", "https://www.reddit.com")

    val postsRDD = subsRDD.flatMap { subscription =>
      try {
        val resolvedUrl = subscription.url.replaceFirst("https://www.reddit.com", baseUrl)
        FileIO.downloadFeed(resolvedUrl) match {
          case None =>
            accFeedsFailed.add(1)
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
            Seq.empty[Post]
          case Some(json) =>
            accFeedsSuccess.add(1)
            val posts = try {
              JsonParser.parsePosts(json, subscription.name)
            } catch {
              case _: Exception =>
                println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
                Seq.empty[Post]
            }
            if (posts.isEmpty) accPostGroupsFailed.add(1)
            else accPostsDownloaded.add(posts.size)
            posts
        }
      } catch {
        case _: Exception =>
          accFeedsFailed.add(1)
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          Seq.empty[Post]
      }
    }.cache()

    val feedsSuccess = accFeedsSuccess.value.toInt
    val feedsFailed = accFeedsFailed.value.toInt
    val postsSuccess = postsRDD.count().toInt
    val postsFailed = accPostGroupsFailed.value.toInt

    val filteredRDD = postsRDD.filter(post => post.title.nonEmpty && post.selftext.nonEmpty && post.selftext.trim.nonEmpty).cache()
    val filteredCount = filteredRDD.count().toInt
    val postsFiltered = postsSuccess - filteredCount

    val totalChars = if (filteredCount > 0) filteredRDD.map(post => post.title.length + post.selftext.length).reduce(_ + _) else 0
    val avgChars = if (filteredCount > 0) totalChars / filteredCount else 0

    val filteredPosts = filteredRDD.collect().toList

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed,
      "postsFiltered" -> postsFiltered,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val entitiesDirFile = new java.io.File(cmdArgs.entitiesDir)
    if (!entitiesDirFile.exists() || !entitiesDirFile.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      return
    }

    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }
}
