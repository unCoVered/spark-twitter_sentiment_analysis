package com.spark.training.spark

import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A Spark Streaming - Flume integration to find Popular hashtags from twitter
  * It receives events from a Flume source that connects to twitter and pushes
  * tweets as avro events to sink.
  *
  * More discussion at stdatalabs.blogspot.com
  *
  * @author Sachin Thirumala
  * Edited by Alejandro Galvez
  */
object FlumePopularHashTags {

  val conf = new SparkConf()
    .setMaster("local[6]")
    .setAppName("Spark Streaming - Flume Source - Popular HashTags")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("WARN")

    val filters = args.takeRight(args.length)
    val ssc = new StreamingContext(sc, Seconds(5))

    // Creates stream using Flume as data source
    val stream = FlumeUtils.createStream(ssc, "localhost", 9988)
    val tweets = stream.map(element => new String(element.event.getBody.array()))

    /**
      * Same code than the used in PopularHashTags.scala
      */
    val hashTags = tweets.flatMap(status => status.split(" ").filter(_.startsWith("#")))

    val mostUsedHashTags60 = hashTags.map((_, 1))
      .reduceByKeyAndWindow((_ + _), Seconds(60))
      .map{ case(hashTag, count) => (count, hashTag)}
      .transform(_.sortByKey(false))

    val mostUsedHashTags10 = hashTags.map((_, 1))
      .reduceByKeyAndWindow((_ + _), Seconds(10))
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

    stream.count().map(cnt => "Received " + cnt + " flume events.").print()

    ssc.start()
    ssc.awaitTermination()

  }
}
