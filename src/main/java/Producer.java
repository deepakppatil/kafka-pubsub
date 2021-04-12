import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class Producer {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		String server = "127.0.0.1:9092";
		String topic = "open_ac";

		Producer producer = new Producer(server);
		producer.put(topic, "oa-100", "OA issuance for 100");
		producer.put(topic, "oa-100", "OA Amendment for 100");

		producer.put(topic, "oa-200", "OA Amendment on 200");
		producer.put(topic, "oa-200", "OA issuance on 200");

		producer.put(topic, "oa-300", "OA Amendment on 97");
		producer.put(topic, "oa-300", "OA issuance on 97");

		producer.close();
	}

	// Variables
	private final KafkaProducer<String, String> producer;
	private final Logger logger = LoggerFactory.getLogger(Producer.class);

	// Constructors
	Producer(String bootstrapServer) {
		Properties props = producerProps(bootstrapServer);
		producer = new KafkaProducer<>(props);
		logger.info("Producer initialized");
	}

	// Public
	void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
		logger.info("Put value: " + value + ", for key: " + key);

		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
		producer.send(record, (recordMetadata, e) -> {
			if (e != null) {
				logger.error("Error while producing", e);
				return;
			}
			logger.info(String.format("eceived new meta. Topic: %s; Partition: %s; Offset: %s; Timestamp: %s",
					recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()));
		}).get();
	}

	void close() {
		logger.info("Closing producer's connection");
		producer.close();
	}

	// Private

	private Properties producerProps(String bootstrapServer) {
		String serializer = StringSerializer.class.getName();
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);

		return props;
	}
}
