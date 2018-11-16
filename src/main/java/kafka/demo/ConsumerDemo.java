package kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka 消费者Demo
 * @ClassName ConsumerDemo
 * @Description TODO
 * @Author Winter
 * @Date 2018/11/16 14:50
 **/
public class ConsumerDemo {


    public KafkaConsumer<String, String> consumer;

    public KafkaConsumer<String, String> getConsumer(String ipPort, String groupId, String topic) {
        Properties props = new Properties();
        props.put("group.id", groupId);
        props.put("bootstrap.servers", ipPort);
        props.put("auto.commit.interval.ms", 1000);
        props.put("enable.auto.commit", true);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }


    public static void main(String[] args) {

		ConsumerDemo conDemo = new ConsumerDemo();
		KafkaConsumer<String, String> consumer = conDemo.getConsumer("KafkaCluster1:9092","test","zytest");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String value = new String(record.value());
                System.out.println(value);
            }
        }
    }
}
