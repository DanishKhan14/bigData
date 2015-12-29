//package org.apache.spark.examples.streaming

//import org.apache.spark.streaming.{Seconds, StreamingContext}
// import org.apache.spark.SparkContext._
// import org.apache.spark.streaming.twitter._
// import org.apache.spark.SparkConf

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.spark.streaming.twitter._


/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object sparkStreaming {
  def main(args: Array[String]) {
    if (args.length < 0) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    //val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    //val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials

    val consumerKey : String = "DUDRKk6U24zRvFTC3VWLtGgeG"
    val consumerSecret : String = "RVE1YbfqDo1IaGEuDG97fh9y9WsJnuWpvaZeZn0oBeU1VnYzzx"
    val accessToken : String = "3982218952-VhTtMYx3A51UGt73ppRfNQ759jRYSRONINLeByN"
    val accessTokenSecret : String = "FQ3gtsJUoL2mIoSViQAzsIgw0fmIpfLu4DFBsmEB9stQD"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
