package com.takzok.kafka;

public class ConsumerController {
  public String consumerGroup;
  public String command;

  public ConsumerController(String consumerGroup, String command){
    this.consumerGroup = consumerGroup;
    this.command = command;
  }
}