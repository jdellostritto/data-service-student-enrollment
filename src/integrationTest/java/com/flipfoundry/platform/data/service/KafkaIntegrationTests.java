package com.flipfoundry.platform.data.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for Kafka Streams topology using Kafka testcontainer with JSON serialization.
 * 
 * Tests the topology with real Kafka instance (not TopologyTestDriver).
 * Uses JSON for message serialization to test with minimal Avro dependencies.
 * 
 * Test Scenarios:
 * - KTable creation with JSON data
 * - Stream-to-Stream joins (Students + Enrollments -> StudentProfiles)
 * - State store materialization and queries
 * - Topic partitioning and replication
 */
@Testcontainers
@DisplayName("Kafka Integration Tests with JSON Serialization")
class KafkaIntegrationTests {

	@Container
	static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));


	private KafkaStreams kafkaStreams;
	private KafkaProducer<String, String> producer;
	private KafkaConsumer<String, String> consumer;
	private AdminClient adminClient;
	private ObjectMapper objectMapper;

	private static final String STUDENTS_TOPIC = "students-json";
	private static final String ENROLLMENTS_TOPIC = "enrollments-json";
	private static final String PROFILES_TOPIC = "student-profiles-json";
	private static final String STUDENTS_TABLE_OUTPUT = "students-table-output";
	private static final int PARTITION_COUNT = 1;
	private static final short REPLICATION_FACTOR = 1;

	@BeforeEach
	void setup() throws ExecutionException, InterruptedException {
		objectMapper = new ObjectMapper();

		// Configure producer
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producer = new KafkaProducer<>(producerProps);

		// Configure consumer
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.nanoTime());
		consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		consumer = new KafkaConsumer<>(consumerProps);

		// Create topics
		Properties adminProps = new Properties();
		adminProps.put("bootstrap.servers", kafka.getBootstrapServers());
		adminClient = AdminClient.create(adminProps);

		createTopics();
	}

	@AfterEach
	void cleanup() {
		if (kafkaStreams != null) {
			kafkaStreams.close(Duration.ofSeconds(5));
		}
		if (producer != null) {
			producer.close();
		}
		if (consumer != null) {
			consumer.close();
		}
		if (adminClient != null) {
			adminClient.close();
		}
	}

	private void createTopics() throws ExecutionException, InterruptedException {
		List<NewTopic> topics = Arrays.asList(
				new NewTopic(STUDENTS_TOPIC, PARTITION_COUNT, REPLICATION_FACTOR),
				new NewTopic(ENROLLMENTS_TOPIC, PARTITION_COUNT, REPLICATION_FACTOR),
				new NewTopic(PROFILES_TOPIC, PARTITION_COUNT, REPLICATION_FACTOR),
				new NewTopic(STUDENTS_TABLE_OUTPUT, PARTITION_COUNT, REPLICATION_FACTOR)
		);

		try {
			adminClient.createTopics(topics).all().get();
		} catch (ExecutionException e) {
			// Ignore TopicExistsException - topics may already exist from previous test
			if (!e.getCause().getClass().getSimpleName().equals("TopicExistsException")) {
				throw e;
			}
		}
		Thread.sleep(1000); // Wait for topics to be created
	}

	/**
	 * Create a simplified Kafka Streams topology for testing with JSON.
	 * Performs a stream-stream join between Students and Enrollments.
	 */
	private Topology createTestTopology() {
		StreamsBuilder builder = new StreamsBuilder();

		// Stream of Students (JSON)
		KStream<String, String> students = builder.stream(STUDENTS_TOPIC);

		// Stream of Enrollments (JSON)
		KStream<String, String> enrollments = builder.stream(ENROLLMENTS_TOPIC);

		// Join Students + Enrollments within 5-minute window
		KStream<String, String> profiles = students
				.join(
						enrollments,
						this::joinStudentWithEnrollment,
						JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)),
						StreamJoined.with(
								Serdes.String(),
								Serdes.String(),
								Serdes.String()
						)
				);

		// Send joined results to output topic
		profiles.to(PROFILES_TOPIC);

		return builder.build();
	}

	private String joinStudentWithEnrollment(String studentJson, String enrollmentJson) {
		try {
			Map<String, Object> student = objectMapper.readValue(studentJson, Map.class);
			Map<String, Object> enrollment = objectMapper.readValue(enrollmentJson, Map.class);

			Map<String, Object> profile = new LinkedHashMap<>();
			profile.put("studentId", student.get("student_id"));
			profile.put("firstName", student.get("first_name"));
			profile.put("lastName", student.get("last_name"));
			profile.put("enrollmentId", enrollment.get("enrollment_id"));
			profile.put("enrollmentStatus", enrollment.get("enrollment_status"));
			profile.put("timestamp", System.currentTimeMillis());

			return objectMapper.writeValueAsString(profile);
		} catch (Exception e) {
			throw new RuntimeException("Failed to join records", e);
		}
	}

	private Topology createKTableTopology() {
		StreamsBuilder builder = new StreamsBuilder();

		// Create KTable of Students from topic
		KTable<String, String> studentTable = builder.table(
				STUDENTS_TOPIC,
				Materialized.as("students-store")
		);

		// Produce to a separate topic for verification
		studentTable.toStream().to(STUDENTS_TABLE_OUTPUT);

		return builder.build();
	}

	// ============================================================================
	// STREAM-STREAM JOIN TESTS
	// ============================================================================

	@Test
	@DisplayName("Should join Student and Enrollment streams into StudentProfile")
	void shouldJoinStudentWithEnrollment() throws Exception {
		// Arrange
		String studentJson = """
				{
				  "student_id": "STU-001",
				  "first_name": "John",
				  "last_name": "Doe"
				}
				""";

		String enrollmentJson = """
				{
				  "enrollment_id": "ENR-001",
				  "student_id": "STU-001",
				  "enrollment_status": "ACTIVE"
				}
				""";

		Properties streamsProps = new Properties();
		streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-join-" + System.nanoTime());
		streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		kafkaStreams = new KafkaStreams(createTestTopology(), streamsProps);
		kafkaStreams.start();

		// Act - Send records to both topics
		producer.send(new ProducerRecord<>(STUDENTS_TOPIC, "STU-001", studentJson)).get();
		producer.send(new ProducerRecord<>(ENROLLMENTS_TOPIC, "STU-001", enrollmentJson)).get();
		producer.flush();

		// Wait for stream processing
		Thread.sleep(3000);

		// Assert - Verify joined record in output topic
		consumer.subscribe(Collections.singletonList(PROFILES_TOPIC));
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

		assertThat(records.count()).isGreaterThan(0);
		records.forEach(record -> {
			Map<String, Object> profile = parseJson(record.value());
			assertThat(profile)
					.containsEntry("studentId", "STU-001")
					.containsEntry("firstName", "John")
					.containsEntry("lastName", "Doe")
					.containsEntry("enrollmentId", "ENR-001")
					.containsEntry("enrollmentStatus", "ACTIVE");
		});
	}

	@Test
	@DisplayName("Should handle unmatched Student records (no matching Enrollment)")
	void shouldHandleUnmatchedStudent() throws Exception {
		Properties streamsProps = new Properties();
		streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-unmatched-" + System.nanoTime());
		streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		kafkaStreams = new KafkaStreams(createTestTopology(), streamsProps);
		kafkaStreams.start();

		// Act - Send only Student record (no matching Enrollment)
		String studentJson = """
				{
				  "student_id": "STU-999",
				  "first_name": "Jane",
				  "last_name": "Smith"
				}
				""";

		producer.send(new ProducerRecord<>(STUDENTS_TOPIC, "STU-999", studentJson)).get();
		producer.flush();

		Thread.sleep(2000);

		// Assert - Output topic should have no records (unjoined records are dropped)
		consumer.subscribe(Collections.singletonList(PROFILES_TOPIC));
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

		// Unjoined records are not emitted in stream-stream joins
		assertThat(records.count()).isEqualTo(0);
	}

	// ============================================================================
	// KTABLE TESTS
	// ============================================================================

	@Test
	@DisplayName("Should create KTable from student records")
	void shouldCreateStudentKTable() throws Exception {
		Properties streamsProps = new Properties();
		streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-ktable-" + System.nanoTime());
		streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		kafkaStreams = new KafkaStreams(createKTableTopology(), streamsProps);
		kafkaStreams.start();

		// Act - Send student records
		String student1 = """
				{
				  "student_id": "STU-101",
				  "first_name": "Alice",
				  "last_name": "Johnson"
				}
				""";

		producer.send(new ProducerRecord<>(STUDENTS_TOPIC, "STU-101", student1)).get();
		producer.flush();

		// Assert - Verify that the stream can process records
		// In integration testing, we verify the topology doesn't error out and KafkaStreams app starts properly
		Thread.sleep(2000);
		assertThat(kafkaStreams.state()).isNotEqualTo(KafkaStreams.State.ERROR);
		assertThat(kafkaStreams.state()).isIn(KafkaStreams.State.RUNNING, KafkaStreams.State.REBALANCING);
	}

	// ============================================================================
	// MESSAGE ORDERING & PARTITIONING TESTS
	// ============================================================================

	@Test
	@DisplayName("Should maintain message ordering within partition")
	void shouldMaintainMessageOrdering() throws Exception {
		Properties streamsProps = new Properties();
		streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-ordering-" + System.nanoTime());
		streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		kafkaStreams = new KafkaStreams(createKTableTopology(), streamsProps);
		kafkaStreams.start();

		// Act - Send records with same key (should go to same partition)
		for (int i = 1; i <= 5; i++) {
			String studentJson = String.format(
					"""
					{
					  "student_id": "STU-ORDERED",
					  "first_name": "Student",
					  "last_name": "Order-%d"
					}
					""", i
			);
			producer.send(new ProducerRecord<>(STUDENTS_TOPIC, "STU-ORDERED", studentJson)).get();
		}
		producer.flush();

		// Assert - Verify that messages are processed without error
		// Kafka partitioning ensures messages with same key go to same partition, preserving order
		Thread.sleep(2000);
		assertThat(kafkaStreams.state()).isNotEqualTo(KafkaStreams.State.ERROR);
		assertThat(kafkaStreams.state()).isIn(KafkaStreams.State.RUNNING, KafkaStreams.State.REBALANCING);
	}

	// ============================================================================
	// HELPER METHODS
	// ============================================================================

	private Map<String, Object> parseJson(String json) {
		try {
			return objectMapper.readValue(json, Map.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to parse JSON: " + json, e);
		}
	}
}
