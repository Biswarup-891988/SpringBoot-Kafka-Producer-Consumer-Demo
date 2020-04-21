package com.kafka.controller;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.KafkaConfig;
import com.kafka.model.SimpleModel;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

  private final KafkaTemplate<String, SimpleModel> kafkaTemplate;

  private final ConcurrentKafkaListenerContainerFactory<String, SimpleModel> kafkaListenerContainerFactory;

  public KafkaController(KafkaTemplate<String, SimpleModel> kafkaTemplate,
      ConcurrentKafkaListenerContainerFactory<String, SimpleModel> kafkaListenerContainerFactory) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
  }

  @PostMapping("/createTopic")
  public void post(@RequestBody SimpleModel simpleModel) {
    kafkaTemplate.send("myTopic", simpleModel);
  }

  @KafkaListener(topics = "myTopic")
  public void getTopicMessage(SimpleModel simpleModel) {
    System.out.println(simpleModel.toString());
  }

}
