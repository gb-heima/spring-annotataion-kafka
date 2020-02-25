package com.gblfy.kafka;

import com.gblfy.utils.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configurable
@Component
@EnableKafka
public class KafkaConsumerConfig {

    Properties properties = PropertiesUtils.read("kafka.properties");

    public KafkaConsumerConfig() {
        System.out.println("kafka消费者配置加载...");
    }

    public Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<String, Object>();

        //Kafka服务地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.consumer.bootstrap.servers"));
        //消费组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("kafka.consumer.group.id"));
        //设置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getProperty("kafka.consumer.enable.auto.commit"));
        //设置间隔时间
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getProperty("kafka.consumer.auto.commit.interval.ms"));
        //Key反序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getProperty("kafka.consumer.key.deserializer"));
        //Value反序列化
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getProperty("kafka.consumer.value.deserializer"));
        //从头开始消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("kafka.consumer.auto.offset.reset"));

        return props;
    }

    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<String, String>(consumerProperties());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }

    @Bean
    public KafkaConsumerListener kafkaConsumerListener() {
        return new KafkaConsumerListener();
    }
}
