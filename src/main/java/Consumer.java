import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class Consumer {

	public static void main(String[] args) {
		String server = "127.0.0.1:9092";
		String groupId = "demo_cg";
		String topic = "open_ac";

		new Consumer(server, groupId, topic).run();
	}

	// Variables

	private final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
	private final String bootServer;
	private final String consGroup;
	private final String kafkaTopic;

	// Constructor

	Consumer(String bootstrapServer, String groupId, String topic) {
		bootServer = bootstrapServer;
		consGroup = groupId;
		kafkaTopic = topic;
	}

	// Public
	void run() {
		logger.info("Creating consumer thread");

		CountDownLatch latch = new CountDownLatch(1);

		ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootServer, consGroup, kafkaTopic, latch);
		Thread thread = new Thread(consumerRunnable);
		thread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			consumerRunnable.shutdown();
			await(latch);

			logger.info("Application has exited");
		}));

		await(latch);
	}

	// Private
	void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}

	// Inner classes
	private class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
			latch = this.latch;

			Properties props = consumerProps(bootstrapServer, groupId);
			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Collections.singletonList(topic));
		}

		@Override
		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : records) {
						logger.info(String.format("Key: %s, Value: %s",  record.key(), record.value()));
						logger.info(String.format("Partition: , Offset: ", record.partition(), record.offset()));
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				consumer.close();
				latch.countDown();
			}
		}

		void shutdown() {
			consumer.wakeup();
		}

		private Properties consumerProps(String bootstrapServer, String groupId) {
			String deserializer = StringDeserializer.class.getName();
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			return properties;
		}
	}
}
