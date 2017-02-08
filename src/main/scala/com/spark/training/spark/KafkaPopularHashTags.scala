package com.spark.training.spark

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A Spark Streaming - Kafka integration to receive twitter
  * data from kafka topic and find the popular hashtags
  *
  * Arguments: <zkQuorum> <consumer-group> <topics> <numThreads>
  * <zkQuorum>       - The zookeeper hostname
  * <consumer-group> - The Kafka consumer group
  * <topics>         - The kafka topic to subscribe to
  * <numThreads>     - Number of kafka receivers to run in parallel
  *
  * More discussion at stdatalabs.blogspot.com
  *
  * @author Sachin Thirumala
  * Edited by Alejandro Galvez
  */
object KafkaPopularHashTags {
  val conf = new SparkConf()
    .setMaster("local[6]")
    .setAppName("Spark Streaming - Kafka Producer - PopularHashTags")
    .set("spark.executor.memory", "1g")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")

    // Array of arguments
    val Array(zkQuorum, group, topics, numThreads) = args

    // Config streaming Context
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Map topic to thread
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // Map value from the kafka message (k, v) pair
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    // Filter hashTags
    val hashTags = lines.flatMap(_.split(" ")).filter(_.startsWith("#"))

    val mostUsedHashTags60 = hashTags.map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{ case (hashTag, count) => (count, hashTag)}
      .transform(_.sortByKey(false))

    val mostUsedHashTags10 = hashTags.map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{ case(hashTag, count) => (count, hashTag)}
      .transform(_.sortByKey(false))

    // Printing
    mostUsedHashTags60.foreachRDD(rdd => {
      val topHashTags = rdd.take(10)
      println("\nPopular topics in the last 60 seconds (%s total):".format(rdd.count()))
      topHashTags.foreach{
        case (count, hashTag) => println("%s (%s tweets)".format(hashTag, count))
      }
    })

    mostUsedHashTags10.foreachRDD(rdd => {
      val topHashTags = rdd.take(10)
      println("\nPopular topics in the last 10 seconds (%s total)".format(rdd.count()))
      topHashTags.foreach{
        case (count, hashTag) => println("%s (%s tweets)".format(hashTag, count))
      }
    })

    lines.count().map(cnt => "Received " + cnt + " kafka messages.").print()

    ssc.start()
    ssc.awaitTermination()
  }
}
