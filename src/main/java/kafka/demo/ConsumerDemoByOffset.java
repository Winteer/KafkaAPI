package kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static kafka.util.txtIOUtil.*;

/**
 * Kafka 消费者Demo （记录offset到本地，防止丢失）
 * @ClassName ConsumerDemoByOffset
 * @Description TODO
 * @Author Winter
 * @Date 2018/11/16 14:52
 **/
public class ConsumerDemoByOffset {

    public KafkaConsumer<String, String> consumer;

    public KafkaConsumer<String, String> getConsumer(String ipPort, String groupId, String topic, String offsetPath) {
        //配置文件
        Properties props = new Properties();
        props.put("group.id", groupId);
        props.put("bootstrap.servers", ipPort);
        props.put("auto.commit.interval.ms", 1000);
        props.put("enable.auto.commit", true);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //从每个partition的本地文件中取出最新的OffSet.(partition数量根据具体创建topic时设置的数量确定)
        long offSetStr0 = Long.parseLong(readLastLine(offsetPath + "/0.txt"));
        long offSetStr1 = Long.parseLong(readLastLine(offsetPath + "/1.txt"));
        long offSetStr2 = Long.parseLong(readLastLine(offsetPath + "/2.txt"));
        long offSetStr3 = Long.parseLong(readLastLine(offsetPath + "/3.txt"));
        long offSetStr4 = Long.parseLong(readLastLine(offsetPath + "/4.txt"));
        long offSetStr5 = Long.parseLong(readLastLine(offsetPath + "/5.txt"));
        long offSetStr6 = Long.parseLong(readLastLine(offsetPath + "/6.txt"));
        long offSetStr7 = Long.parseLong(readLastLine(offsetPath + "/7.txt"));
        long offSetStr8 = Long.parseLong(readLastLine(offsetPath + "/8.txt"));
        long offSetStr9 = Long.parseLong(readLastLine(offsetPath + "/9.txt"));

        TopicPartition p0 = new TopicPartition(topic, 0);
        TopicPartition p1 = new TopicPartition(topic, 1);
        TopicPartition p2 = new TopicPartition(topic, 2);
        TopicPartition p3 = new TopicPartition(topic, 3);
        TopicPartition p4 = new TopicPartition(topic, 4);
        TopicPartition p5 = new TopicPartition(topic, 5);
        TopicPartition p6 = new TopicPartition(topic, 6);
        TopicPartition p7 = new TopicPartition(topic, 7);
        TopicPartition p8 = new TopicPartition(topic, 8);
        TopicPartition p9 = new TopicPartition(topic, 9);
        List<TopicPartition> list = new ArrayList();
        list.add(p0);
        list.add(p1);
        list.add(p2);
        list.add(p3);
        list.add(p4);
        list.add(p5);
        list.add(p6);
        list.add(p7);
        list.add(p8);
        list.add(p9);
        consumer.assign(list);
        consumer.seek(p0, offSetStr0);
        consumer.seek(p1, offSetStr1);
        consumer.seek(p2, offSetStr2);
        consumer.seek(p3, offSetStr3);
        consumer.seek(p4, offSetStr4);
        consumer.seek(p5, offSetStr5);
        consumer.seek(p6, offSetStr6);
        consumer.seek(p7, offSetStr7);
        consumer.seek(p8, offSetStr8);
        consumer.seek(p9, offSetStr9);
        return consumer;
    }

    public static void main(String[] args) {
        ConsumerDemoByOffset coff = new ConsumerDemoByOffset();
        KafkaConsumer<String, String> consumer = coff.getConsumer("KafkaCluster1:9092","test","zytest","F:\\Kafka\\KafkaOffsetLog");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String value = new String(record.value());
                System.out.println(value);
                // 记录topic下每个partition的OffSet到本地文件
                try {
                    rewriteendline("F:/Kafka/KafkaOffsetLog/" + record.partition() + ".txt", String.valueOf(record.offset()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
