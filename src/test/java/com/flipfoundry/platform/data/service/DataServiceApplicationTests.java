package com.flipfoundry.platform.data.service;

import com.flipfoundry.platform.data.service.avro.Student;
import com.flipfoundry.platform.data.service.avro.StudentEnrollment;
import com.flipfoundry.platform.data.service.kafka.utils.SerdesFactory;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

/**
 * TopologyTestDriver Unit Tests for Student Data Service Kafka Streams topology.
 * 
 * Tests verify:
 * - KTable creation and materialization
 * - KTable reduce logic (keeps latest by timestamp)
 * - Serialization/Deserialization with Avro Serdes
 * - Invalid record routing to DLQ
 * - State store queries
 */
@DisplayName("Kafka Streams Topology Unit Tests")
class DataServiceApplicationTests {
	private static final String SCHEMA_REGISTRY_SCOPE = DataServiceApplicationTests.class.getName();
	private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

	private TopologyTestDriver testDriver;

	private TestInputTopic<byte[], byte[]> studentsInTopic;
	private TestInputTopic<byte[], byte[]> enrollmentsInTopic;
	private TestOutputTopic<byte[], byte[]> studentsDlqTopic;
	private TestOutputTopic<byte[], byte[]> enrollmentsDlqTopic;

	private SerdesFactory serdesFactory;
	private Serde<String> stringSerde;
	private SpecificAvroSerde<Student> studentSerde;
	private SpecificAvroSerde<StudentEnrollment> enrollmentSerde;

	@BeforeEach
	void beforeEach() throws Exception {
		serdesFactory = new SerdesFactory();
		serdesFactory.setSrUrl(MOCK_SCHEMA_REGISTRY_URL);

		stringSerde = Serdes.String();
		studentSerde = serdesFactory.studentSpecificAvroSerde();
		enrollmentSerde = serdesFactory.studentEnrollmentSpecificAvroSerde();

		// Create topology manually (simplified from KtableProcessor for testing)
		Topology topology = createTestTopology();

		// Configure test driver
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		props.put("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);

		testDriver = new TopologyTestDriver(topology, props);

		// Create input topics (raw bytes)
		Serde<byte[]> bytesSerde = Serdes.ByteArray();
		studentsInTopic = testDriver.createInputTopic("students", bytesSerde.serializer(), bytesSerde.serializer());
		enrollmentsInTopic = testDriver.createInputTopic("student-enrollments", bytesSerde.serializer(), bytesSerde.serializer());

		// Create output topics for DLQ
		studentsDlqTopic = testDriver.createOutputTopic("students-dlq", bytesSerde.deserializer(), bytesSerde.deserializer());
		enrollmentsDlqTopic = testDriver.createOutputTopic("enrollments-dlq", bytesSerde.deserializer(), bytesSerde.deserializer());
	}

	@AfterEach
	void afterEach() {
		testDriver.close();
		MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
	}

	/**
	 * Create a simplified topology for testing that mirrors KtableProcessor behavior
	 */
	private Topology createTestTopology() {
		StreamsBuilder builder = new StreamsBuilder();
		Serde<byte[]> bytesSerde = Serdes.ByteArray();

		// ============================================================================
		// STUDENTS STREAM PROCESSING
		// ============================================================================
		
		// Branch on valid/invalid deserialization
		KStream<byte[], byte[]>[] partitionedStudents = builder.stream("students", Consumed.with(bytesSerde, bytesSerde))
				.branch(
						// Valid records pass deserialization
						(k, v) -> canDeserialize(v, studentSerde),
						// Invalid records go to DLQ
						(k, v) -> true
				);

		// Invalid records to DLQ
		partitionedStudents[1].to("students-dlq");

		// Valid students: deserialize, group, reduce by latest timestamp, materialize
		partitionedStudents[0]
				.mapValues((v) -> studentSerde.deserializer().deserialize("students", v))
				.selectKey((k, v) -> v != null ? v.getStudentId().toString() : "unknown")
				.groupByKey(Grouped.with(stringSerde, studentSerde))
				.reduce(
						(s1, s2) -> {
							if (s1 == null) return s2;
							if (s2 == null) return s1;
							long ts1 = s1.getUpdatedAt() != null ? s1.getUpdatedAt() : 0L;
							long ts2 = s2.getUpdatedAt() != null ? s2.getUpdatedAt() : 0L;
							return (ts1 >= ts2) ? s1 : s2;
						},
							Materialized.with(stringSerde, studentSerde).as("STUDENTS-MV")
				);

		// ============================================================================
		// STUDENT ENROLLMENTS STREAM PROCESSING
		// ============================================================================
		
		// Branch on valid/invalid deserialization
		KStream<byte[], byte[]>[] partitionedEnrollments = builder.stream("student-enrollments", Consumed.with(bytesSerde, bytesSerde))
				.branch(
						// Valid records pass deserialization
						(k, v) -> canDeserialize(v, enrollmentSerde),
						// Invalid records go to DLQ
						(k, v) -> true
				);

		// Invalid records to DLQ
		partitionedEnrollments[1].to("enrollments-dlq");

		// Valid enrollments: deserialize, group, reduce by latest timestamp, materialize
		partitionedEnrollments[0]
				.mapValues((v) -> enrollmentSerde.deserializer().deserialize("student-enrollments", v))
				.selectKey((k, v) -> v != null ? v.getEnrollmentId().toString() : "unknown")
				.groupByKey(Grouped.with(stringSerde, enrollmentSerde))
				.reduce(
						(e1, e2) -> {
							if (e1 == null) return e2;
							if (e2 == null) return e1;
							long ts1 = e1.getUpdatedAt() != null ? e1.getUpdatedAt() : 0L;
							long ts2 = e2.getUpdatedAt() != null ? e2.getUpdatedAt() : 0L;
							return (ts1 >= ts2) ? e1 : e2;
						},
							Materialized.with(stringSerde, enrollmentSerde).as("ENROLLMENTS-MV")
				);

		return builder.build();
	}

	private boolean canDeserialize(byte[] value, SpecificAvroSerde<?> serde) {
		if (value == null) return false;
		try {
			serde.deserializer().deserialize("test-topic", value);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	// ============================================================================
	// STUDENT KTABLE TESTS
	// ============================================================================

	@Nested
	@DisplayName("Student KTable Tests")
	class StudentKTableTests {

		@Test
		@DisplayName("Should create Student KTable entry from valid record")
		void shouldCreateStudentKTable() {
			// Arrange
			Student student = Student.newBuilder()
					.setStudentId("STU-001")
					.setStateStudentId("CA-12345")
					.setFirstName("John")
					.setLastName("Doe")
					.setDateOfBirth("2010-05-15")
					.setGradeLevel("10")
					.setUpdatedAt(System.currentTimeMillis())
					.build();

			byte[] studentBytes = studentSerde.serializer().serialize("students", student);

			// Act
			studentsInTopic.pipeInput(student.getStudentId().toString().getBytes(), studentBytes);

			// Assert
			ReadOnlyKeyValueStore<String, Student> store = testDriver.getKeyValueStore("STUDENTS-MV");
			Student retrieved = store.get("STU-001");
			assertThat(retrieved)
					.isNotNull()
					.hasFieldOrPropertyWithValue("studentId", "STU-001")
					.hasFieldOrPropertyWithValue("firstName", "John")
					.hasFieldOrPropertyWithValue("lastName", "Doe");
		}

		@Test
		@DisplayName("Should keep latest Student record (by timestamp) when duplicate arrives")
		void shouldKeepLatestStudentByTimestamp() {
			// Arrange
			long olderTime = System.currentTimeMillis() - 10000;
			long newerTime = System.currentTimeMillis();

			Student olderStudent = Student.newBuilder()
					.setStudentId("STU-002")
					.setFirstName("John")
					.setLastName("Doe")
					.setUpdatedAt(olderTime)
					.build();

			Student newerStudent = Student.newBuilder()
					.setStudentId("STU-002")
					.setFirstName("Jane")
					.setLastName("Doe")
					.setUpdatedAt(newerTime)
					.build();

			byte[] olderBytes = studentSerde.serializer().serialize("students", olderStudent);
			byte[] newerBytes = studentSerde.serializer().serialize("students", newerStudent);

			// Act
			studentsInTopic.pipeInput("STU-002".getBytes(), olderBytes);
			studentsInTopic.pipeInput("STU-002".getBytes(), newerBytes);

			// Assert - should keep the newer one (Jane)
			ReadOnlyKeyValueStore<String, Student> store = testDriver.getKeyValueStore("STUDENTS-MV");
			Student retrieved = store.get("STU-002");
			assertThat(retrieved)
					.isNotNull()
					.hasFieldOrPropertyWithValue("firstName", "Jane")
					.hasFieldOrPropertyWithValue("updatedAt", newerTime);
		}

		@Test
		@DisplayName("Should keep older Student record when newer one has older timestamp")
		void shouldKeepRecordWithLatestTimestamp() {
			// Arrange
			long newerTime = System.currentTimeMillis();
			long olderTime = newerTime - 5000;

			Student firstRecord = Student.newBuilder()
					.setStudentId("STU-003")
					.setFirstName("Alice")
					.setLastName("Smith")
					.setUpdatedAt(newerTime)
					.build();

			Student secondRecord = Student.newBuilder()
					.setStudentId("STU-003")
					.setFirstName("Bob")
					.setLastName("Jones")
					.setUpdatedAt(olderTime)  // This is older
					.build();

			byte[] firstBytes = studentSerde.serializer().serialize("students", firstRecord);
			byte[] secondBytes = studentSerde.serializer().serialize("students", secondRecord);

			// Act
			studentsInTopic.pipeInput("STU-003".getBytes(), firstBytes);
			studentsInTopic.pipeInput("STU-003".getBytes(), secondBytes);

			// Assert - should keep the first one (Alice, with newer timestamp)
			ReadOnlyKeyValueStore<String, Student> store = testDriver.getKeyValueStore("STUDENTS-MV");
			Student retrieved = store.get("STU-003");
			assertThat(retrieved)
					.isNotNull()
					.hasFieldOrPropertyWithValue("firstName", "Alice");
		}
	}

	// ============================================================================
	// ENROLLMENT KTABLE TESTS
	// ============================================================================

	@Nested
	@DisplayName("StudentEnrollment KTable Tests")
	class EnrollmentKTableTests {

		@Test
		@DisplayName("Should create StudentEnrollment KTable entry from valid record")
		void shouldCreateEnrollmentKTable() {
			// Arrange
			StudentEnrollment enrollment = StudentEnrollment.newBuilder()
					.setEnrollmentId("ENR-001")
					.setStudentId("STU-001")
					.setSchoolId("SCH-001")
					.setDistrictId("DIST-001")
					.setSchoolYear("2025-2026")
					.setEntryDate("2025-08-01")
					.setExitDate(null)
					.setEnrollmentStatus("ACTIVE")
					.setUpdatedAt(System.currentTimeMillis())
					.build();

			byte[] enrollmentBytes = enrollmentSerde.serializer().serialize("student-enrollments", enrollment);

			// Act
			enrollmentsInTopic.pipeInput(enrollment.getEnrollmentId().toString().getBytes(), enrollmentBytes);

			// Assert
			ReadOnlyKeyValueStore<String, StudentEnrollment> store = testDriver.getKeyValueStore("ENROLLMENTS-MV");
			StudentEnrollment retrieved = store.get("ENR-001");
			assertThat(retrieved)
					.isNotNull()
					.hasFieldOrPropertyWithValue("enrollmentId", "ENR-001")
					.hasFieldOrPropertyWithValue("studentId", "STU-001")
					.hasFieldOrPropertyWithValue("enrollmentStatus", "ACTIVE");
		}

		@Test
		@DisplayName("Should update StudentEnrollment status in KTable when newer record arrives")
		void shouldUpdateEnrollmentStatus() {
			// Arrange
			long time1 = System.currentTimeMillis() - 5000;
			long time2 = System.currentTimeMillis();

			StudentEnrollment enrollment1 = StudentEnrollment.newBuilder()
					.setEnrollmentId("ENR-002")
					.setStudentId("STU-002")
					.setSchoolId("SCH-001")
					.setDistrictId("DIS-001")
					.setSchoolYear("2025-2026")
					.setEnrollmentStatus("ACTIVE")
					.setUpdatedAt(time1)
					.build();

			StudentEnrollment enrollment2 = StudentEnrollment.newBuilder()
					.setEnrollmentId("ENR-002")
					.setStudentId("STU-002")
					.setSchoolId("SCH-001")
					.setDistrictId("DIS-001")
					.setSchoolYear("2025-2026")
					.setEnrollmentStatus("INACTIVE")
					.setExitDate("2026-06-30")
					.setUpdatedAt(time2)
					.build();

			byte[] bytes1 = enrollmentSerde.serializer().serialize("student-enrollments", enrollment1);
			byte[] bytes2 = enrollmentSerde.serializer().serialize("student-enrollments", enrollment2);

			// Act
			enrollmentsInTopic.pipeInput("ENR-002".getBytes(), bytes1);
			enrollmentsInTopic.pipeInput("ENR-002".getBytes(), bytes2);

			// Assert - should have updated to INACTIVE
			ReadOnlyKeyValueStore<String, StudentEnrollment> store = testDriver.getKeyValueStore("ENROLLMENTS-MV");
			StudentEnrollment retrieved = store.get("ENR-002");
			assertThat(retrieved)
					.isNotNull()
					.hasFieldOrPropertyWithValue("enrollmentStatus", "INACTIVE")
					.hasFieldOrPropertyWithValue("exitDate", "2026-06-30");
		}
	}

	// ============================================================================
	// INVALID RECORD / DLQ TESTS
	// ============================================================================

	@Nested
	@DisplayName("Dead Letter Queue (DLQ) Tests")
	class DLQTests {

		@Test
		@DisplayName("Should route invalid Student records to DLQ")
		void shouldRouteMalformedStudentToDLQ() {
			// Arrange - send invalid bytes that cannot be deserialized as Avro
			byte[] invalidBytes = "not-an-avro-record".getBytes();

			// Act
			studentsInTopic.pipeInput("invalid-key".getBytes(), invalidBytes);

			// Assert - should have been sent to students-dlq
			assertThat(studentsDlqTopic.getQueueSize()).isGreaterThan(0);
		}

		@Test
		@DisplayName("Should route invalid StudentEnrollment records to DLQ")
		void shouldRouteMalformedEnrollmentToDLQ() {
			// Arrange
			byte[] invalidBytes = "garbage-data".getBytes();

			// Act
			enrollmentsInTopic.pipeInput("invalid-key".getBytes(), invalidBytes);

			// Assert
			assertThat(enrollmentsDlqTopic.getQueueSize()).isGreaterThan(0);
		}
	}

	// ============================================================================
	// STATE STORE QUERY TESTS
	// ============================================================================

	@Nested
	@DisplayName("State Store Query Tests")
	class StateStoreTests {

		@Test
		@DisplayName("Should be able to iterate all Student records in KTable")
		void shouldIterateAllStudentRecords() {
			// Arrange
			Student student1 = Student.newBuilder()
					.setStudentId("STU-100")
					.setFirstName("Alice")
					.setLastName("Smith")
					.setUpdatedAt(System.currentTimeMillis())
					.build();

			Student student2 = Student.newBuilder()
					.setStudentId("STU-101")
					.setFirstName("Bob")
					.setLastName("Jones")
					.setUpdatedAt(System.currentTimeMillis())
					.build();

			byte[] bytes1 = studentSerde.serializer().serialize("students", student1);
			byte[] bytes2 = studentSerde.serializer().serialize("students", student2);

			// Act
			studentsInTopic.pipeInput("STU-100".getBytes(), bytes1);
			studentsInTopic.pipeInput("STU-101".getBytes(), bytes2);

			// Assert
			ReadOnlyKeyValueStore<String, Student> store = testDriver.getKeyValueStore("STUDENTS-MV");
			try (KeyValueIterator<String, Student> iterator = store.all()) {
				int count = 0;
				while (iterator.hasNext()) {
					count++;
					iterator.next();
				}
				assertThat(count).isEqualTo(2);
			}
		}

		@Test
		@DisplayName("Should handle range queries on Student KTable")
		void shouldPerformRangeQuery() {
			// Arrange
			Student student1 = Student.newBuilder().setStudentId("STU-A").setFirstName("Alice").setLastName("Smith").setUpdatedAt(System.currentTimeMillis()).build();
			Student student2 = Student.newBuilder().setStudentId("STU-B").setFirstName("Bob").setLastName("Jones").setUpdatedAt(System.currentTimeMillis()).build();
			Student student3 = Student.newBuilder().setStudentId("STU-C").setFirstName("Charlie").setLastName("Brown").setUpdatedAt(System.currentTimeMillis()).build();

			byte[] bytes1 = studentSerde.serializer().serialize("students", student1);
			byte[] bytes2 = studentSerde.serializer().serialize("students", student2);
			byte[] bytes3 = studentSerde.serializer().serialize("students", student3);

			// Act
			studentsInTopic.pipeInput("STU-A".getBytes(), bytes1);
			studentsInTopic.pipeInput("STU-B".getBytes(), bytes2);
			studentsInTopic.pipeInput("STU-C".getBytes(), bytes3);

			// Assert - range from STU-A to STU-B (inclusive)
			ReadOnlyKeyValueStore<String, Student> store = testDriver.getKeyValueStore("STUDENTS-MV");
			try (KeyValueIterator<String, Student> iterator = store.range("STU-A", "STU-B")) {
				int count = 0;
				while (iterator.hasNext()) {
					count++;
					iterator.next();
				}
				assertThat(count).isEqualTo(2);
			}
		}
	}
}


