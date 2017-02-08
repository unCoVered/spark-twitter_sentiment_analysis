package com.spark.training.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaTwitterProducer {

  public static void main (String[] args) throws InterruptedException {
    final String CONSUMER_KEY = "BFdcHpyoxgLerfkADnj8N9vvl";
    final String CONSUMER_SECRET = "wP5o8Oq1qfRoTh8z5GpCGY2A7YHwBWfb0XsULgUnmzr0ujuLI1";
    final String ACCESS_TOKEN = "365704012-qJJiXGx2r4Y5EoQWAMC5acrXOzOCsm602iNbTHT6";
    final String ACCESS_TOKEN_SECRET = "ptdPfNtRNPxtSLY3lpY3IxIYAeDiapOb8DToF1LeSZcxX";
    final String TOPIC_NAME = args[0];

    final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

    String[] arguments = args.clone();
    String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);

    // Set twitter oAuth tokens in the configuration
    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(true)
        .setOAuthConsumerKey(CONSUMER_KEY)
        .setOAuthConsumerSecret(CONSUMER_SECRET)
        .setOAuthAccessToken(ACCESS_TOKEN)
        .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);

    // Twitter Stream
    TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
    StatusListener statusListener = new StatusListener() {
      @Override public void onStatus(Status status) {
        queue.offer(status);
      }

      @Override  public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
      }

      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
      }

      public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
      }

      public void onStallWarning(StallWarning stallWarning) {
        System.out.println("Got stall warning:" + stallWarning);
      }

      public void onException(Exception ex) {
        ex.printStackTrace();
      }
    };

    twitterStream.addListener(statusListener);

    // Filter Keywords
    FilterQuery filterQuery = new FilterQuery().track(keyWords);
    twitterStream.filter(filterQuery);

    // Thread.sleep(5000);

    // Add Kafka producer
    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<String, String>(props);
    int i = 0;
    int j = 0;

    // poll for new tweets in the queue. If new tweets are added, send them
    // to the topic
    while (true) {
      Status ret = queue.poll();

      if (ret == null) {
        Thread.sleep(100);
        // i++;
      } else {
        for (HashtagEntity hashtage : ret.getHashtagEntities()) {
          System.out.println("Tweet:" + ret);
          System.out.println("Hashtag: " + hashtage.getText());
          // producer.send(new ProducerRecord<String, String>(
          // topicName, Integer.toString(j++), hashtage.getText()));
          producer.send(new ProducerRecord<String, String>(TOPIC_NAME, Integer.toString(j++), ret.getText()));
        }
      }
    }
    // producer.close();
    // Thread.sleep(500);
    // twitterStream.shutdown();
  }
}
