package dragon.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataProducer {

	public static void main(String[] args) {
		new DataProducer().run();
	}

	void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", Constant.bootstrapServers);
		props.put("acks", "all");
		props.put("delivery.timeout.ms", 50000);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 5; i++) {
			// producer.send(new ProducerRecord<String, String>("aeon-1-1", "info-" + Integer.toString(i)));
			// producer.send(new ProducerRecord<String, String>("aeon-1-2", "info-" + Integer.toString(i)));
			// producer.send(new ProducerRecord<String, String>("aeon-2-1", "info-" + Integer.toString(i)));
			// producer.send(new ProducerRecord<String, String>("aeon-2-3", "info-" + Integer.toString(i)));
			producer.send(new ProducerRecord<String, String>("aeon", "info-" + Integer.toString(i)));
		}

		producer.close();
	}

}
