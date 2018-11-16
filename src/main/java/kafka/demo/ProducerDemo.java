package kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka 生产者Demo
 * @ClassName ProducerDemo
 * @Description TODO
 * @Author Winter
 * @Date 2018/11/16 10:42
 **/
public class ProducerDemo {
    public KafkaProducer<String,String> kafkaProducer;

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public  ProducerDemo(String brokerList) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList.substring(1));//格式：host1:port1,host2:port2,....
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);//a batch size of zero will disable batching entirely
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);//send message without delay
        props.put(ProducerConfig.ACKS_CONFIG, "1");//对应partition的leader写到本地后即返回成功。极端情况下，可能导致失败
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<String, String>(props);
    }

    public void sendMessage(String topic, List<String> messages) throws InterruptedException, ExecutionException {
        for(String tmpmess : messages) {
            ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic, tmpmess);
            this.kafkaProducer.send(message).get();
        }
    }

    public static void main(String[] args){
        List<String> messages = new ArrayList<>();
        messages.add("this is the first message.");
        messages.add("this is the second message.");
        ProducerDemo producerDemo = new ProducerDemo("KafkaCluster1:9092,KafkaCluster2:9092,KafkaCluster3:9092");
        try {
            producerDemo.sendMessage("zytest",messages);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
