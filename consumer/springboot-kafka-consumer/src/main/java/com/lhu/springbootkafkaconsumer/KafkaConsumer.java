package com.lhu.springbootkafkaconsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

  private static final String TOPIC_FOR_STRING_MSG = "lhu-string-msg-topic";
  private static final String TOPIC_FOR_JSON_MSG = "lhu-json-msg-topic";

  @KafkaListener(
      topics = TOPIC_FOR_STRING_MSG,
      groupId = KafkaConsumerConfig.STR_MSG_CONSUMER_GRP_1)
  public void consumeStrMessage(String message) {
    System.out.println("Topic lhu-string-msg-topic | message = " + message);
  }

  // @Payload String record,@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition)
  @KafkaListener(topics = TOPIC_FOR_JSON_MSG, groupId = KafkaConsumerConfig.JSON_MSG_CONSUMER_GRP_1)
  public void consumeJsonMessage(String message) {
    try {
      Student student = new ObjectMapper().readValue(message, Student.class);
      System.out.println("Topic lhu-json-msg-topic | message = " + student.toString());
    } catch (Exception e) {
      System.out.println(
          "Topic lhu-json-msg-topic consumed.But fail to convert the message to object | message = "
              + message);
    }
  }
}
