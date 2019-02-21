package com.takzok.kafka.consumer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import com.takzok.kafka.KafkaClientInterface;
import com.takzok.kafka.util.PropertyUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Consumer implements KafkaClientInterface {

  private static final Logger logger = Logger.getLogger(Consumer.class);
  private static final String PROPERTY_FILE = "consumer.properties";

   @Option(required = true, name = "-p", aliases = "--props", metaVar = "Property file directory", usage = "Enter the directory where the 'producer.properties' file exists.")
  public String properties;
  @Option(required = true, name = "-t", aliases = "--topic", metaVar = "Topic name", usage = "Enter topic name to consume.")
  public String topicName;

  @Option(name = "-g", aliases = "--group", metaVar = "Consumer group", usage = "Enter group name of the consumer. The option value get preference over group.id specified in the consuer.properties.")
  public String consumerGroup;
  @Option(name = "-i", aliases = "--consumer-id", metaVar = "Consumer ID", usage = "Enter consumer identification name. The option value get preference over consumer.id specified in the consuer.properties.")
  public String clientId = "kafka-java-consumer";

  @Option(name = "-h", aliases = "--help", metaVar = "Help", usage = "Print usage message and exit")
  private boolean usageFlag = false;

  public void consumeMessage(String topic) throws IOException {
    final Thread mainThread = Thread.currentThread();
    Properties props = new PropertyUtil(new File(properties, PROPERTY_FILE).getPath()).readProperty();
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
    ArrayList<String> topicList = new ArrayList<String>();
    topicList.add(topic);
    System.out.println("topic: " + topic + " added to topic list.");
    kafkaConsumer.subscribe(topicList);
    
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Exiting...");
      kafkaConsumer.wakeup();
      try {
        mainThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));
    
    try {
      final int WAIT_TIME = 1000;
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(WAIT_TIME);
        for (ConsumerRecord<String, String> record : records) {
          logger.log(Level.INFO,
          "Message: " + record.value() + ",Partition: " + record.partition() + ", offset: " + record.offset());
          // for manual commit.
          kafkaConsumer.commitSync();
        }
      }
    } catch (WakeupException e) {
      System.out.println("WakeupException is thrown. But ignore this. This client may be killed by user.");
    } catch (final Exception e) {
      e.printStackTrace();
    } finally {
      kafkaConsumer.close();
    }
  }
  
  @Override
  public void execute(String[] args) {
    Consumer consumer = new Consumer();
    
    CmdLineParser parser = new CmdLineParser(consumer);
    try {
      if (args.length == 0) {
        logger.log(Level.ERROR, "Enter topic name");
        parser.printUsage(System.out);
        return;
      }
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      logger.log(Level.ERROR, e.getMessage());
      parser.printUsage(System.out);
      return;
    }
    
    if (consumer.usageFlag) {
      parser.printUsage(System.out);
      return;
    }
    // Consume
    try {
      consumer.consumeMessage(consumer.topicName);
    } catch (IOException e) {
      logger.log(Level.ERROR,"Failed to open the property file: " + e.getMessage());
      return;
    }
  }
}
