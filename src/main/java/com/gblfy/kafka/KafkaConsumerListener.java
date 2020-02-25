package com.gblfy.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

public class KafkaConsumerListener {

    @KafkaListener(topics = "testTopic01")
    public void listen01(ConsumerRecord<String,String> consumerRecord){
        System.out.println("开始消费testTopic01的消息");
        System.out.println("消费者线程："+Thread.currentThread().getName()+"[ 消息 来自kafkatopic："+consumerRecord.topic()+",分区："+consumerRecord.partition() +" ，委托时间："+consumerRecord.timestamp()+"]消息内容如下：");
        System.out.println(consumerRecord.value());
    }

    @KafkaListener(topics = "testTopic02")
    public void listen02(ConsumerRecord<String,String> consumerRecord){
        System.out.println("开始消费testTopic02的消息");
        System.out.println(consumerRecord.value());
    }
}
