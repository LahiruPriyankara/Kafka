package com.lhu.springboot.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;

@RestController
public class KafkaProducerController {

    @Autowired
    KafkaTemplate<String, Student> kafkaTemplate;

    private static final String TOPIC_FOR_STRING_MSG = "lhu-string-msg-topic";
    private static final String TOPIC_FOR_JSON_MSG = "lhu-json-msg-topic";

    @PostMapping("/publish/string-message")
    public String publishMessage(@RequestBody Student student) {
        kafkaTemplate.send(TOPIC_FOR_STRING_MSG, student);
        return "Message Successfully Published To Topic " + TOPIC_FOR_STRING_MSG;
    }

    @PostMapping("/publish/json-object")
    public String publishMessageObject(@RequestBody Student student) {
        Message<Student> message = MessageBuilder.withPayload(student).setHeader(KafkaHeaders.TOPIC, TOPIC_FOR_JSON_MSG).build();
        kafkaTemplate.send(message);
        return "Message Successfully Published To Topic " + TOPIC_FOR_JSON_MSG;
    }
}
