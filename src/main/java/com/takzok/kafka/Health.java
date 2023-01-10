package com.takzok.kafka;

import com.takzok.kafka.consumer.SuspendableConsumer;
import com.takzok.kafka.producer.Producer;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Health {

  private static final int OK = 200;
  private static final int ERROR = 500;

  private SuspendableConsumer kafkaConsumer;
  private Producer kafkaProducer;
  private HttpServer server;

  public Health(SuspendableConsumer  kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }

  public Health(Producer kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
    
  }

  public void start() {
    try {
      server = HttpServer.create(new InetSocketAddress("localhost", 8080), 0);
    } catch (IOException ioe) {
      throw new RuntimeException("Could not setup http server: ", ioe);
    }
    server.createContext("/health", exchange -> {
      int responseCode = 0;
      if (kafkaConsumer != null){
        responseCode = kafkaConsumer.isRunning() ? OK : ERROR;
      }else if(kafkaProducer != null){
        responseCode = kafkaProducer.isRunning() ? OK : ERROR;
      }
      exchange.sendResponseHeaders(responseCode, 0);
      exchange.close();
    });
    server.start();
  }

  public void stop() {
    server.stop(0);
  }
}