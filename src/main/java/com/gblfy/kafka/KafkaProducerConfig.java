package com.gblfy.kafka;

import com.gblfy.utils.PropertiesUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka 消息生产者配置
 */
@Configurable
@Component
@EnableKafka
public class KafkaProducerConfig {

    Properties properties = PropertiesUtils.read("kafka.properties");

    public KafkaProducerConfig() {
        System.out.println("kafka 生产者配置加载...");
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory(producerProperties());
    }

    public Map<String, Object> producerProperties() {
        Map<String, Object> props = new HashMap<String, Object>();
        //Kafka服务地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.producer.bootstrap.servers"));
        //设置当前客户端id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, properties.getProperty("kafka.producer.client.id"));
        //设置消费端确认机制
        props.put(ProducerConfig.ACKS_CONFIG, properties.getProperty("kafka.producer.acks"));
        //发送失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, properties.getProperty("kafka.producer.retries"));
        //批处理条数,当多个记录被发送至统一分区时，producer对于同一个分区来说，会按照 batch.size 的大小进行统一收集，批量发送
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getProperty("kafka.producer.batch.size"));
        //与 batch.size 配合使用。延迟统一收集，产生聚合，然后批量发送至broker
        props.put(ProducerConfig.LINGER_MS_CONFIG,properties.getProperty("kafka.producer.linger.ms"));
        //Key序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProperty("kafka.producer.key.serializer"));
        //Value序列化
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProperty("kafka.producer.value.serializer"));

        return props;
    }

    @Bean
    public KafkaTemplate<String,String> kafkaTemplate(){
        KafkaTemplate<String,String> kafkaTemplate = new KafkaTemplate<String, String>(producerFactory(),true);
        //设置默认的topic(此处可做一些具体设置)
        kafkaTemplate.setDefaultTopic(properties.getProperty("kafka.producer.defaultTopic"));
        return kafkaTemplate;
    }


}
