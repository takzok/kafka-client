package com.takzok.kafka.producer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.takzok.kafka.Health;
import com.takzok.kafka.KafkaClientInterface;
import com.takzok.kafka.util.PropertyUtil;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Producer implements KafkaClientInterface {
  private static final Logger logger = Logger.getLogger(Producer.class);
  private static final String PROPERTY_FILE = "producer.properties";
  private static boolean state = false;

  @Option(required = true, name = "-p", aliases = "--props", metaVar = "Property file directory", usage = "Enter the directory where the 'producer.properties' file exists.")
  public String properties;
  @Option(required = true, name = "-t", aliases = "--topic", metaVar = "Topic Name", usage = "Enter the topic name to produce record.")
  public String topicName;
  @Option(name = "-a", aliases = "--asynchronously", metaVar = "Send asynchronously", usage = "Set to send record asynchronously. Defaut: False(Send records synchronously.) If --transactional is true, this option is ignored.")
  public boolean isAsync = false;
  // @Option(name = "-c", aliases = "--commit-num", metaVar = "Numbers of messages in one commit", usage = "Specify the number of messages to include in one commit.)")
  // public int commitNum = 1;
  @Option(name = "-i", aliases = "--interval", metaVar = "Send interval between record produce", usage = "Set interval between record produce.  Defaut: 500ms")
  public int sleepTime = 500;
  @Option(name = "-n", aliases = "--records", metaVar = "Record number", usage = "Number of records to produce. Default: 10")
  public int numOfRecords = 10;
  @Option(name = "-s", aliases = "--size", metaVar = "Record size", usage = "Record size to produce. Default: 1024")
  public int recordSize = 1024;
  @Option(name = "-x", aliases = "--transactional", metaVar = "Send message as a transaction", usage = "Set to send record as a transaction. Defaut: False(Send records non transactional.)")
  public boolean isTransactional = false;


  @Option(name = "-h", aliases = "--help", metaVar = "help", usage = "Print usage message and exit")
  private boolean usageFlag;

  public void asyncProducer(String topic, String payload, int records, String properties) throws IOException {

    Properties props = new PropertyUtil(new File(properties, PROPERTY_FILE).getPath()).readProperty();
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
    try {
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, payload);
      logger.log(Level.INFO, "Attempt to send a record asynchronously.");
      for (int i = 0; i < records; i++) {
        // Send record asynchronously
        kafkaProducer.send(record, (metadata, exception) -> {
          if (metadata != null) {
        logger.log(Level.INFO, "Message produced, offset: " + metadata.offset()
            + ", partition: " + metadata.partition() + ", timestamp: " + metadata.timestamp() );
          } else {
            exception.printStackTrace();
          }
        });
        Thread.sleep(sleepTime);
      }
    } catch (final Exception e) {
        logger.log(Level.ERROR, e.getStackTrace());
        e.printStackTrace();
    } finally {
      kafkaProducer.close();
    }
  }

  public void syncProducer(String topic, String payload, int records, String properties) throws IOException {
    Properties props = new PropertyUtil(new File(properties, PROPERTY_FILE).getPath()).readProperty();
    logger.log(Level.INFO, props.entrySet());
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

    try {
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, payload);
      logger.log(Level.INFO, "Attempt to send a record synchronously.");
      for (int i = 0; i < records; i++) {
        // Send record synchronously
        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
        logger.log(Level.INFO, "Message produced, payload: " + payload + ", offset: " + recordMetadata.offset()
            + ", partition: " + recordMetadata.partition() + ", partitonsInfo: " + kafkaProducer.partitionsFor(topic));
        Thread.sleep(sleepTime);
      }
    } catch (InterruptedException e) {
      logger.log(Level.ERROR, e.getMessage());
    } catch (ExecutionException e) {
      logger.log(Level.ERROR, e.getMessage());
    } finally {
      kafkaProducer.close();
      logger.log(Level.INFO, "Kafka producer connection closed.");
    }
  }

  public void transactionalProducer(String topic, String payload, int records, String properties) throws IOException {
    Properties props = new PropertyUtil(new File(properties, PROPERTY_FILE).getPath()).readProperty();
    logger.log(Level.INFO, props.entrySet());
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
    logger.log(Level.INFO, "init transaction.");
    kafkaProducer.initTransactions();
    Health health = new Health(this);
    health.start();
    try {
      kafkaProducer.beginTransaction();
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, payload);
      logger.log(Level.INFO, "Attempt to send a record transactional.");
      for (int i = 0; i < records; i++) {
        // Send record synchronously
        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
        logger.log(Level.INFO, "Message produced, payload: " + payload + ", offset: " + recordMetadata.offset()
            + ", partition: " + recordMetadata.partition() + ", partitonsInfo: " + kafkaProducer.partitionsFor(topic));
        Thread.sleep(sleepTime);
      }
      kafkaProducer.commitTransaction();
    } catch (InterruptedException e) {
      logger.log(Level.ERROR, e.getMessage());
      e.printStackTrace();

      kafkaProducer.abortTransaction();
    } catch (ExecutionException e) {
      logger.log(Level.ERROR, e.getMessage());
      e.printStackTrace();
      kafkaProducer.abortTransaction();
    } finally {
      kafkaProducer.close();
      logger.log(Level.INFO, "Kafka producer connection closed.");
    }
  }
  
  public String recordBuilder(int recordSize) {
    String record = StringUtils.upperCase(RandomStringUtils.randomAlphanumeric(recordSize));
    logger.log(Level.INFO, "Record payload: " + record);
    return record;
  }
  
  @Override
  public void execute(String[] args) {
    
    Producer producer = new Producer();
    CmdLineParser parser = new CmdLineParser(producer);
    try {
      if (args.length == 0) {
        parser.printUsage(System.out);
        return;
      }
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      logger.log(Level.ERROR, e.getMessage());
      parser.printUsage(System.out);
      return;
    }
    //    if (producer.usageFlag) {
      //      parser.printUsage(System.out);
      //      return;
      //    }
      
    logger.log(Level.INFO, "Create Producer");
    // Produce
    try {
      if(isTransactional){
        producer.transactionalProducer(producer.topicName, producer.recordBuilder(producer.recordSize), producer.numOfRecords, producer.properties);
      }else if (isAsync) {
        producer.asyncProducer(producer.topicName, producer.recordBuilder(producer.recordSize), producer.numOfRecords, producer.properties);
      } else {
        producer.syncProducer(producer.topicName, producer.recordBuilder(producer.recordSize), producer.numOfRecords, producer.properties);
      }
    } catch (IOException e) {
      logger.log(Level.ERROR,"Failed to open the property file: " + e.getMessage());
      return;
    }
    logger.log(Level.INFO,"finished.");
  }
}