package com.spark.training

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A Spark Streaming application that receives tweets on certain
  * keywords from twitter datasource and find the popular hashtags
  *
  * Arguments: <keyword_1> ... <keyword_n>
  * <keyword_1>          - The keyword to filter tweets
  * <keyword_n>          - Any number of keywords to filter tweets
  *
  * More discussion at stdatalabs.blogspot.com
  *
  * @author Sachin Thirumala
  * Edited by Alejandro Galvez
  */
object PopularHashTags {

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("Spark Streaming - PopularHashTags")

  val sc = new SparkContext(conf)

  val CONSUMER_KEY = "BFdcHpyoxgLerfkADnj8N9vvl"
  val CONSUMER_SECRET = "wP5o8Oq1qfRoTh8z5GpCGY2A7YHwBWfb0XsULgUnmzr0ujuLI1"
  val ACCESS_TOKEN = "365704012-qJJiXGx2r4Y5EoQWAMC5acrXOzOCsm602iNbTHT6"
  val ACCESS_TOKEN_SECRET = "ptdPfNtRNPxtSLY3lpY3IxIYAeDiapOb8DToF1LeSZcxX"

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("WARN")

    val filters = args.takeRight(args.length)

    // System properties -> Twitter keys
    System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET)

    val ssc = new StreamingContext(sc, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    /**
      * HashTags -> Takes the words which start with #
      * MostUsed -> Creates tuple (hashTag, count) -> Swap the elements of the tuple
      *   -> and sort by count
      */
    val hashTags = stream.flatMap(tweet => tweet.getText.split(" ").filter(_.startsWith("#")))

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

    ssc.start()
    ssc.awaitTermination()
  }
}
