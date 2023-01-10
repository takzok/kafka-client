package com.takzok.kafka.consumer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import com.google.gson.Gson;
import com.takzok.kafka.ConsumerController;
import com.takzok.kafka.Health;
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

public class SuspendableConsumer implements KafkaClientInterface {

  private static final Logger logger = Logger.getLogger(SuspendableConsumer.class);
  private static final String PROPERTY_FILE = "consumer.properties";
  private static final String CONTROLLER_PROPERTY_FILE = "consumerController.properties";
  private static boolean state = false;

  @Option(required = true, name = "-p", aliases = "--props", metaVar = "Property file directory", usage = "Enter the directory where the 'producer.properties' file exists.")
  public String properties;
  @Option(required = true, name = "-c", aliases = "--controller-props", metaVar = "Property file directory", usage = "Enter the directory where the 'consumercontroller.properties' file exists.")
  public String controllerProperties;
  @Option(required = true, name = "-t", aliases = "--topic", metaVar = "Topic name", usage = "Enter topic name to consume.")
  public String topicName;

  @Option(name = "-g", aliases = "--group", metaVar = "Consumer group", usage = "Enter group name of the consumer. The option value get preference over group.id specified in the consuer.properties.")
  public String consumerGroup;
  @Option(name = "-i", aliases = "--consumer-id", metaVar = "Consumer ID", usage = "Enter consumer identification name. The option value get preference over consumer.id specified in the consuer.properties.")
  public String clientId = "kafka-java-consumer";

  @Option(name = "-h", aliases = "--help", metaVar = "Help", usage = "Print usage message and exit")
  private boolean usageFlag = false;

  public void consumeMessage(String topic) throws IOException {

    Health health = new Health(this);
    health.start();
    final Thread mainThread = Thread.currentThread();
    Properties props = new PropertyUtil(new File(properties, PROPERTY_FILE).getPath()).readProperty();
    Properties controllerProps = new PropertyUtil(new File(properties, CONTROLLER_PROPERTY_FILE).getPath()).readProperty();
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
    KafkaConsumer<String, String> kafkaConsumerController = new KafkaConsumer<String, String>(controllerProps);
    ArrayList<String> topicList = new ArrayList<String>();
    ArrayList<String> controllerTopicList = new ArrayList<String>();
    String CONSUMER_CONTROLLER = "consumer_controller";
    topicList.add(topic);
    System.out.println("TOPIC ASSIGNMENT: " + kafkaConsumer.assignment());
    System.out.println("topic: " + topic + " added to topic list.");
    controllerTopicList.add(CONSUMER_CONTROLLER);
    System.out.println("topic: " + CONSUMER_CONTROLLER + " is consumed for consumer controller");
    kafkaConsumer.subscribe(topicList);
    kafkaConsumerController.subscribe(controllerTopicList);
    
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Exiting...");
      kafkaConsumer.wakeup();
      kafkaConsumerController.wakeup();
      try {
        mainThread.join();
        health.stop();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));
    updateState(kafkaConsumer);
    try {
      Thread.sleep(15000);
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    
    try {
      final Duration WAIT_TIME = Duration.ofSeconds(0);
      Gson gson = new Gson();
      Thread.sleep(30000);
      while (true) {
        updateState(kafkaConsumer);
        ConsumerRecords<String, String> controlRecords = kafkaConsumerController.poll(WAIT_TIME);
        if (controlRecords == null){
          System.out.println("no controll records found.");
        }else if(controlRecords != null){
          for (ConsumerRecord<String, String> record : controlRecords) {
            logger.log(Level.INFO, "Controll Record: " + record.value());
            ConsumerController cc = gson.fromJson(record.value(), ConsumerController.class);
            if (cc.consumerGroup.equals(props.getProperty("group.id"))) {
              if (cc.command.equals("pause")){
                kafkaConsumer.pause(kafkaConsumer.assignment());
                System.out.println("consumer for " + kafkaConsumer.assignment() + "is paused.");
                kafkaConsumerController.commitSync();
              }
              else if(cc.command.equals("resume")){
                kafkaConsumer.resume(kafkaConsumer.paused());
                System.out.println("consumer for " + kafkaConsumer.paused() + "is resumed.");
                kafkaConsumerController.commitSync();
              }
            }
          }
        }
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
      kafkaConsumerController.close();
    }
  }
  
  @Override
  public void execute(String[] args) {
    SuspendableConsumer consumer = new SuspendableConsumer();
    
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

  public void updateState(KafkaConsumer kafkaConsumer){
    if (kafkaConsumer == null) {
      state = false;
    }else{
      // check topic-partition list that this consumer is assigned to.
      if(kafkaConsumer.assignment().isEmpty()){
        state = false;
      }else{
        state = true;
      }
    }
  }
  public boolean isRunning() {
    return state;
  }
}

