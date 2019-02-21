package com.takzok.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.takzok.kafka.consumer.*;
import com.takzok.kafka.producer.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;


public class KafkaClient {
  private static final Logger logger = Logger.getLogger(KafkaClient.class);
  
  @Argument(handler = SubCommandHandler.class, required = true, metaVar = "ClientType", usage = "Client type")
  @SubCommands({
    @SubCommand(name = "Producer", impl = Producer.class),
    @SubCommand(name = "Consumer", impl = Consumer.class)
  })
//  @Option(name = "-h", aliases = "--help", usage = "Print usage message and exit")
//  private boolean usageFlag = false;
  
  private KafkaClientInterface command;
  
  public static void main(String[] args) {
    KafkaClient kafkaClient = new KafkaClient();
    // Exit if no argument is passed
    if (args.length == 0) {
      System.out.println("Enter client type");
      return;
    }
    CmdLineParser parser = new CmdLineParser(kafkaClient);
    try {
      parser.parseArgument(args);
      kafkaClient.command.execute(removeSubCmdFromArgs(args));
    } catch (CmdLineException e){
      logger.log(Level.ERROR, "Arguments parse error.");
      logger.log(Level.ERROR, e.getMessage());
      return;
    } catch (NullPointerException e){
      logger.log(Level.ERROR, e.getMessage());
      parser.printUsage(System.out);
    }
  }
  public static String[] removeSubCmdFromArgs(String[] args){
    List<String> argList = new ArrayList<>(Arrays.asList(args));
    argList.remove(args[0]);
    
    return argList.toArray(new String[0]); 
  }
}