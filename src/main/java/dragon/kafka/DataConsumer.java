package dragon.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class DataConsumer {

	public static void main(String[] args) {
		new DataConsumer().run();
	}

	void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", Constant.bootstrapServers);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		Map<String, List<PartitionInfo>> topics = consumer.listTopics();
		for (Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
			if (entry.getKey().equals("__consumer_offsets")) {
				continue;
			}
			System.out.println(entry.getKey());
			for (PartitionInfo info : entry.getValue()) {
				System.out.println("\t" + info);
			}
		}

		TopicPartition topicPartition = new TopicPartition("aeon", 0);
		consumer.assign(Arrays.asList(topicPartition));
		System.out.println("Assignment Set: " + consumer.assignment());
		System.out.println("Position on " + topicPartition + ": " + consumer.position(topicPartition));
		consumer.seekToBeginning(Arrays.asList(topicPartition));

		// consumer.subscribe(Arrays.asList("test"));

		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
		for (ConsumerRecord<String, String> record : records) {
			System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}

	}

}
