package com.takzok.kafka.producer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

public class Producer implements KafkaClientInterface{
  private static final Logger logger = Logger.getLogger(Producer.class);
  private static final String PROPERTY_FILE = "producer.properties";
  
  @Option(required = true, name = "-p", aliases = "--props", metaVar = "Property file directory", usage = "Enter the directory where the 'producer.properties' file exists.")
  public String properties;
  @Option(required = true, name = "-t", aliases = "--topic", metaVar = "Topic Name", usage = "Enter the topic name to produce record.")
  public String topicName;
  
  @Option(name = "-a", aliases = "--asynchronously", metaVar = "Send asynchronously" ,usage = "Set to send record asynchronously. Defaut: False(Send records synchronously.")
  public boolean asyncFlag= false;
  @Option(name = "-n", aliases = "--records", metaVar = "Record number" ,usage = "Number of records to produce. Default: 10")
  public int numOfRecords = 10;
  @Option(name = "-s", aliases = "--size", metaVar = "Record size" , usage = "Record size to produce. Default: 1024")
  public int recordSize = 1024;
  
  @Option(name = "-h", aliases = "--help", metaVar = "help", usage = "Print usage message and exit")
  private boolean usageFlag;
  
  public void asyncProducer(String topic, String payload, int records, String properties) throws IOException{
    
    Properties props = new PropertyUtil(new File(properties, PROPERTY_FILE).getPath()).readProperty();
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
    try {
      for (int i = 0; i < records; i++) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, payload);
        // Send record asynchronously
        kafkaProducer.send(record);
        System.out.println("record sended.");
      }
    } catch (final Exception e) {
      e.printStackTrace();
    } finally {
      kafkaProducer.close();
    }
  }
  
  public void syncProducer(String topic, String payload, int records, String properties) throws IOException{
    Properties props = new PropertyUtil(properties + PROPERTY_FILE).readProperty();
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
    
    try {
      for (int i = 0; i < records; i++) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, payload);
        // Send record synchronously
        kafkaProducer.send(record);
        System.out.println("record sended.");
        Future<RecordMetadata> future = kafkaProducer.send(record);
        RecordMetadata recordMetadata = future.get(1000,
        TimeUnit.MILLISECONDS);
        logger.log(Level.INFO, "Message produced, msg payload: " + payload + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
      }
    } catch (final Exception e) {
      e.printStackTrace();
    } finally {
      kafkaProducer.close();
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
        if (asyncFlag) {
          producer.asyncProducer(producer.topicName, producer.recordBuilder(producer.recordSize), producer.numOfRecords, producer.properties);
        } else {
          producer.syncProducer(producer.topicName, producer.recordBuilder(producer.recordSize), producer.numOfRecords, producer.properties);
        }
      } catch (IOException e) {
      logger.log(Level.ERROR,"Failed to open the property file: " + e.getMessage());
        return;
      }
      System.out.println("finished.");
    }
  }
