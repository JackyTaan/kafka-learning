package htbv.com.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        //

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String  bootstrapServers = "127.0.0.1:9092";

        //System.out.println("Hello world!");
        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++){
            //create producer  record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hello world" + Integer.toString(i));
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully send or an exception is thrown
                    if (e == null){
                        //the record as successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing", e);
                    }
                }
            });
        }


        //flush data
        producer.flush();
        //close
        producer.close();
    }
}
