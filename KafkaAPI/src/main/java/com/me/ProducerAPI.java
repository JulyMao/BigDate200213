package com.me;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author maow
 * @create 2020-05-08 11:52
 */
public class ProducerAPI {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("acks","all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("bootstrap.servers", "hadoop102:9092");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
//            Thread.sleep(10);
            producer.send(new ProducerRecord<String, String>(
                    "second",
                    "Message" + i,
                    "maow这是第" + i + "条信息"
            ), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null){
                        String topic = recordMetadata.topic();
                        int partition = recordMetadata.partition();
                        long offset = recordMetadata.offset();
                        System.out.println(
                                topic + "话题"
                                        + partition + "分区第"
                                        + offset + "条消息发送成功"
                        );
                    }
                }
            });
            System.out.println("这是第"+ i +"条数据");
        }
        producer.close();
    }
}
