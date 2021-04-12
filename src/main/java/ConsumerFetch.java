import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerFetch {

	public static void main(String[] args) {
		String server = "127.0.0.1:9092";
		String topic = "user_registered";
		long offset = 15L;
		int partitionNum = 0;
		int numOfMessages = 5;

		new ConsumerFetch(server, topic).run(offset, partitionNum, numOfMessages);
	}

	// Variables
	private final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
	private final String bootServer;
	private final String kafkaTopic;

	// Constructor
	private ConsumerFetch(String bootstrapServer, String topic) {
		bootServer = bootstrapServer;
		kafkaTopic = topic;
	}

	// Public
	void run(long offset, int partitionNum, int numOfMessages) {
		Properties props = consumerProps(bootServer);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		setupConsumer(consumer, offset, partitionNum);
		fetchMessages(consumer, numOfMessages);
	}

	// Private
	private void setupConsumer(KafkaConsumer<String, String> consumer, long offset, int partitionNum) {
		TopicPartition partition = new TopicPartition(kafkaTopic, partitionNum);
		consumer.assign(Collections.singletonList(partition));
		consumer.seek(partition, offset);
	}

	private void fetchMessages(KafkaConsumer<String, String> consumer, int numOfMessages) {
		int numberOfMessagesRead = 0;
		boolean keepOnReading = true;

		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				numberOfMessagesRead += 1;

				logger.info("Key: " + record.key() + ", Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

				if (numberOfMessagesRead >= numOfMessages) {
					keepOnReading = false;
					break;
				}
			}
		}
	}

	private Properties consumerProps(String bootstrapServer) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return properties;
	}
}
