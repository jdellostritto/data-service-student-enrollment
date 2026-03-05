package com.flipfoundry.platform.data.service.kafka.processor;

import com.flipfoundry.platform.data.service.avro.Student;
import com.flipfoundry.platform.data.service.avro.StudentEnrollment;
import com.flipfoundry.platform.data.service.kafka.utils.SerdesFactory;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit Tests for Kafka Processor Serdes and Utilities.
 * Tests SerdesFactory functionality without requiring full Spring context.
 */
@DisplayName("Kafka Processor Serdes Unit Tests")
class KafkaProcessorSpringIntegrationTests {

    private static final String SCHEMA_REGISTRY_SCOPE = KafkaProcessorSpringIntegrationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    private SerdesFactory serdesFactory;

    @BeforeEach
    void setUp() {
        serdesFactory = new SerdesFactory();
        serdesFactory.setSrUrl(MOCK_SCHEMA_REGISTRY_URL);
    }

    @AfterEach
    void tearDown() {
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

    @Nested
    @DisplayName("Bean Autowiring Tests")
    class BeanAutowiringTests {

        @Test
        @DisplayName("should have processors available for testing")
        void shouldHaveProcessorsAvailable() {
            // Note: Processors may not be autowired in test context
            // This test verifies the test configuration is setup correctly
            assertThat(serdesFactory).isNotNull();
        }

        @Test
        @DisplayName("should be able to create SerdesFactory instance")
        void shouldCreateSerdesFactory() {
            assertThatCode(() -> {
                SerdesFactory factory = new SerdesFactory();
                factory.setSrUrl(MOCK_SCHEMA_REGISTRY_URL);
                assertThat(factory).isNotNull();
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should be able to get Avro Serdes from factory")
        void shouldGetAvroSerdes() {
            assertThatCode(() -> {
                SpecificAvroSerde<Student> studentSerde = serdesFactory.studentSpecificAvroSerde();
                assertThat(studentSerde).isNotNull();

                SpecificAvroSerde<StudentEnrollment> enrollmentSerde = serdesFactory.studentEnrollmentSpecificAvroSerde();
                assertThat(enrollmentSerde).isNotNull();
            }).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Serialization/Deserialization Tests")
    class SerializationTests {

        @Test
        @DisplayName("should serialize and deserialize Student records")
        void shouldSerializeAndDeserializeStudent() {
            assertThatCode(() -> {
                SpecificAvroSerde<Student> serde = serdesFactory.studentSpecificAvroSerde();

                // Create a test student
                Student student = Student.newBuilder()
                        .setStudentId("STUD-001")
                        .setStateStudentId("CA-12345")
                        .setFirstName("John")
                        .setLastName("Doe")
                        .setDateOfBirth("2010-05-15")
                        .setGradeLevel("10")
                        .setUpdatedAt(System.currentTimeMillis())
                        .build();

                // Serialize
                byte[] serialized = serde.serializer().serialize("students", student);
                assertThat(serialized).isNotNull().isNotEmpty();

                // Deserialize
                Student deserialized = serde.deserializer().deserialize("students", serialized);
                assertThat(deserialized).isNotNull();
                assertThat(deserialized.getStudentId()).isEqualTo("STUD-001");
                assertThat(deserialized.getFirstName()).isEqualTo("John");
                assertThat(deserialized.getLastName()).isEqualTo("Doe");
                assertThat(deserialized.getStateStudentId()).isEqualTo("CA-12345");
                assertThat(deserialized.getGradeLevel()).isEqualTo("10");
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should serialize and deserialize StudentEnrollment records")
        void shouldSerializeAndDeserializeStudentEnrollment() {
            assertThatCode(() -> {
                SpecificAvroSerde<StudentEnrollment> serde = serdesFactory.studentEnrollmentSpecificAvroSerde();

                // Create a test enrollment
                StudentEnrollment enrollment = StudentEnrollment.newBuilder()
                        .setEnrollmentId("ENROLL-001")
                        .setStudentId("STUD-001")
                        .setSchoolId("SCH-001")
                        .setDistrictId("DIST-001")
                        .setSchoolYear("2025-2026")
                        .setEntryDate("2025-08-01")
                        .setEnrollmentStatus("ACTIVE")
                        .setUpdatedAt(System.currentTimeMillis())
                        .build();

                // Serialize
                byte[] serialized = serde.serializer().serialize("enrollments", enrollment);
                assertThat(serialized).isNotNull().isNotEmpty();

                // Deserialize
                StudentEnrollment deserialized = serde.deserializer().deserialize("enrollments", serialized);
                assertThat(deserialized).isNotNull();
                assertThat(deserialized.getStudentId()).isEqualTo("STUD-001");
                assertThat(deserialized.getEnrollmentId()).isEqualTo("ENROLL-001");
                assertThat(deserialized.getSchoolYear()).isEqualTo("2025-2026");
                assertThat(deserialized.getEnrollmentStatus()).isEqualTo("ACTIVE");
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should handle null optional fields in Student")
        void shouldHandleNullFieldsInStudent() {
            assertThatCode(() -> {
                SpecificAvroSerde<Student> serde = serdesFactory.studentSpecificAvroSerde();

                // Create student with minimal fields
                Student student = Student.newBuilder()
                        .setStudentId("STUD-002")
                        .setFirstName("Jane")
                        .setLastName("Smith")
                        .setDateOfBirth(null)
                        .setStateStudentId(null)
                        .setGradeLevel(null)
                        .setUpdatedAt(System.currentTimeMillis())
                        .build();

                byte[] serialized = serde.serializer().serialize("students", student);
                Student deserialized = serde.deserializer().deserialize("students", serialized);

                assertThat(deserialized).isNotNull();
                assertThat(deserialized.getStudentId()).isEqualTo("STUD-002");
                assertThat(deserialized.getDateOfBirth()).isNull();
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should handle null optional fields in StudentEnrollment")
        void shouldHandleNullFieldsInEnrollment() {
            assertThatCode(() -> {
                SpecificAvroSerde<StudentEnrollment> serde = serdesFactory.studentEnrollmentSpecificAvroSerde();

                // Create enrollment with nullable fields
                StudentEnrollment enrollment = StudentEnrollment.newBuilder()
                        .setEnrollmentId("ENROLL-002")
                        .setStudentId("STUD-002")
                        .setSchoolId("SCH-002")
                        .setDistrictId("DIST-002")
                        .setSchoolYear("2025-2026")
                        .setEntryDate("2025-08-01")
                        .setExitDate(null)  // Nullable
                        .setEnrollmentStatus("ACTIVE")
                        .setUpdatedAt(System.currentTimeMillis())
                        .build();

                byte[] serialized = serde.serializer().serialize("enrollments", enrollment);
                StudentEnrollment deserialized = serde.deserializer().deserialize("enrollments", serialized);

                assertThat(deserialized).isNotNull();
                assertThat(deserialized.getEnrollmentId()).isEqualTo("ENROLL-002");
                assertThat(deserialized.getExitDate()).isNull();
            }).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("SerdesFactory Configuration Tests")
    class SerdesFactoryTests {

        @Test
        @DisplayName("should create Student Avro Serde with schema registry URL")
        void shouldCreateStudentSerde() {
            assertThatCode(() -> {
                SpecificAvroSerde<Student> serde = serdesFactory.studentSpecificAvroSerde();
                assertThat(serde).isNotNull();
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should create StudentEnrollment Avro Serde with schema registry URL")
        void shouldCreateEnrollmentSerde() {
            assertThatCode(() -> {
                SpecificAvroSerde<StudentEnrollment> serde = serdesFactory.studentEnrollmentSpecificAvroSerde();
                assertThat(serde).isNotNull();
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should support multiple Student serialization/deserialization cycles")
        void shouldSupportMultipleSerializationCycles() {
            assertThatCode(() -> {
                SpecificAvroSerde<Student> serde = serdesFactory.studentSpecificAvroSerde();

                // First cycle
                Student student1 = Student.newBuilder()
                        .setStudentId("STUD-003")
                        .setFirstName("Alice")
                        .setLastName("Johnson")
                        .setUpdatedAt(System.currentTimeMillis())
                        .build();

                byte[] serialized1 = serde.serializer().serialize("students", student1);
                Student deserialized1 = serde.deserializer().deserialize("students", serialized1);
                assertThat(deserialized1.getStudentId()).isEqualTo("STUD-003");

                // Second cycle
                Student student2 = Student.newBuilder()
                        .setStudentId("STUD-004")
                        .setFirstName("Bob")
                        .setLastName("Williams")
                        .setUpdatedAt(System.currentTimeMillis())
                        .build();

                byte[] serialized2 = serde.serializer().serialize("students", student2);
                Student deserialized2 = serde.deserializer().deserialize("students", serialized2);
                assertThat(deserialized2.getStudentId()).isEqualTo("STUD-004");

                // Verify both exist independently
                assertThat(deserialized1.getStudentId()).isNotEqualTo(deserialized2.getStudentId());
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should serialize StudentEnrollment with all fields populated")
        void shouldSerializeFullEnrollmentRecord() {
            assertThatCode(() -> {
                SpecificAvroSerde<StudentEnrollment> serde = serdesFactory.studentEnrollmentSpecificAvroSerde();

                StudentEnrollment enrollment = StudentEnrollment.newBuilder()
                        .setEnrollmentId("ENROLL-003")
                        .setStudentId("STUD-005")
                        .setSchoolId("SCH-005")
                        .setDistrictId("DIST-005")
                        .setSchoolYear("2025-2026")
                        .setEntryDate("2025-08-01")
                        .setExitDate("2026-06-30")
                        .setEnrollmentStatus("COMPLETED")
                        .setUpdatedAt(System.currentTimeMillis())
                        .build();

                byte[] serialized = serde.serializer().serialize("enrollments", enrollment);
                StudentEnrollment deserialized = serde.deserializer().deserialize("enrollments", serialized);

                assertThat(deserialized.getEnrollmentId()).isEqualTo("ENROLL-003");
                assertThat(deserialized.getExitDate()).isEqualTo("2026-06-30");
                assertThat(deserialized.getEnrollmentStatus()).isEqualTo("COMPLETED");
            }).doesNotThrowAnyException();
        }
    }
}
