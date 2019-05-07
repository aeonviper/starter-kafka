package dragon.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

public class DataDeleter {

	public static void main(String[] args) {
		new DataDeleter().run();
	}

	void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", Constant.bootstrapServers);

		TopicPartition topicPartition = new TopicPartition("aeon", 0);

		AdminClient adminClient = AdminClient.create(props);
		Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
		recordsToDelete.put(topicPartition, RecordsToDelete.beforeOffset(-1));
		DeleteRecordsResult result = adminClient.deleteRecords(recordsToDelete);
		Map<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks = result.lowWatermarks();
		try {
			for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry : lowWatermarks.entrySet()) {
				System.out.println(entry.getKey().topic() + " " + entry.getKey().partition() + " " + entry.getValue().get().lowWatermark());
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		adminClient.close();
	}
}
