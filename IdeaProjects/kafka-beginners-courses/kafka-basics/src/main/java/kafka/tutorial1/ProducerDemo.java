package htbv.com.kafka.tutorial1;

import oracle.jvm.hotspot.jfr.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.lwawt.macosx.CSystemTray;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //System.out.println("Hello world!");
        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer  record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hello world");
        //send data
        producer.send(record);
        //flush data
        producer.flush();
        //close
        producer.close();
    }
}
