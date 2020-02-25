package com.gblfy.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("kafka")
public class KafkaController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    /**
     * 消息发送
     */
    @RequestMapping("producer")
    @ResponseBody
    public void producer(){
        kafkaTemplate.send("testTopic01","producer发送消息01");
        kafkaTemplate.send("testTopic02","producer发送消息02");
    }

}
