package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class ProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "note01:9092,note02:9092,note03:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        //读取日志文件写入kafka中
        try{
            BufferedReader reader = new BufferedReader(new FileReader(new File("C:\\Users\\admin\\Desktop\\data\\移动项目\\flumeLoggerapp4.log.20170412")));
            String line;
            while (null != (line = reader.readLine())) {
                System.out.println(line);
                producer.send(new ProducerRecord<String, String>("test", line));
                Thread.sleep(2000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        producer.close();
    }
}
