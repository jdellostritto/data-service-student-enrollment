package com.flipfoundry.platform.data.service.kafka.processor;

import com.flipfoundry.platform.data.service.avro.Student;
import com.flipfoundry.platform.data.service.avro.StudentEnrollment;
import com.flipfoundry.platform.data.service.kafka.utils.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

/**
 * KTable Processor for Student and StudentEnrollment streams.
 * Constructs KTables from Student and StudentEnrollment topics for use in downstream joins.
 * 
 * Patterns:
 * - Consumes raw bytes from Kafka topics
 * - Branches streams on deserialization validity (good vs DLQ)
 * - Deserializes to Avro objects
 * - Creates KTables with reduce logic (keeps latest by timestamp)
 * - Materializes state stores for interactive queries
 */
@Slf4j
@EnableKafka
@EnableKafkaStreams
@Component
@ConfigurationProperties
public class KtableProcessor {

    @Value("${kafka.topics.students:students}")
    private String studentTopic;

    @Value("${kafka.topics.enrollments:student-enrollments}")
    private String enrollmentTopic;

    @Autowired
    SerdesFactory serdesFactory;

    /**
     * The KTable For Students
     */
    private KTable<String, Student> studentKTable;

    /**
     * @return KTable for Students
     */
    public KTable<String, Student> getStudentKTable() {
        return studentKTable;
    }

    /**
     * The KTable For Student Enrollments
     */
    private KTable<String, StudentEnrollment> studentEnrollmentKTable;

    /**
     * @return KTable for Student Enrollments
     */
    public KTable<String, StudentEnrollment> getStudentEnrollmentKTable() {
        return studentEnrollmentKTable;
    }

    /**
     * The Streams Builder process method
     * Builds KTables for both Student and StudentEnrollment streams
     *
     * @param builder StreamsBuilder
     */
    @Autowired
    public void process(StreamsBuilder builder) {
        var bytesSerde = Serdes.ByteArray();
        var stringSerde = Serdes.String();

        // ============================================================================
        // STUDENTS STREAM PROCESSING
        // ============================================================================
        log.info("Processing Student stream from topic: {}", studentTopic);
        
        // STAGE 1a: Student deserialization validation - separate valid and invalid streams
        KStream<byte[], byte[]> inputStudents = builder.stream(studentTopic, Consumed.with(bytesSerde, bytesSerde));
        inputStudents.peek((key, value) -> log.info("Consume Student Key: {} value: {}", key, value));
        
        // Predicate for valid student records
        java.util.function.Predicate<? super byte[]> isValidStudent = v -> {
            try {
                if (v == null) {
                    return false;
                } else {
                    serdesFactory.studentSpecificAvroSerde().deserializer().deserialize(studentTopic, v);
                    return true;
                }
            } catch (SerializationException ignored) {
                log.error("Student deserialization error: {}", ignored.getMessage());
                return false;
            }
        };
        
        // STAGE 1b: Branch into valid and invalid streams
        KStream<byte[], byte[]> validStudentStream = inputStudents.filter((k, v) -> isValidStudent.test(v));
        KStream<byte[], byte[]> invalidStudentStream = inputStudents.filterNot((k, v) -> isValidStudent.test(v));

        // Send invalid records to DLQ
        invalidStudentStream.to("students-dlq", Produced.with(bytesSerde, bytesSerde));

        // STAGE 2: Deserialize valid student records to Avro
        final KStream<String, Student> studentAvroStream = validStudentStream
                .map((key, value) -> {
                    String studentId = null;
                    Student student = null;
                    try {
                        student = serdesFactory.studentSpecificAvroSerde().deserializer().deserialize(studentTopic, value);
                        studentId = student.getStudentId() != null ? student.getStudentId().toString() : new String(key);
                    } catch (SerializationException e) {
                        log.error("Failed to deserialize Student: {}", e.getMessage());
                        studentId = new String(key);
                    }
                    return KeyValue.pair(studentId, student);
                });

        // STAGE 3: Create Student KTable with reduce logic (latest by timestamp)
        studentKTable = studentAvroStream
                .selectKey((key, value) -> value != null ? value.getStudentId().toString() : key)
                .groupByKey(Grouped.with(stringSerde, serdesFactory.studentSpecificAvroSerde()))
                .reduce((student1, student2) -> {
                    if (student1 == null) return student2;
                    if (student2 == null) return student1;
                    
                    long ts1 = student1.getUpdatedAt() != null ? student1.getUpdatedAt() : 0L;
                    long ts2 = student2.getUpdatedAt() != null ? student2.getUpdatedAt() : 0L;
                    log.info("Student reduce - Student1 timestamp: {} Student2 timestamp: {}", ts1, ts2);
                    return (ts1 > ts2) ? student1 : student2;
                })
                .toStream()
                .toTable(Materialized.<String, Student, KeyValueStore<Bytes, byte[]>>
                        as("STUDENTS-MV")
                        .withKeySerde(stringSerde)
                        .withValueSerde(serdesFactory.studentSpecificAvroSerde()));

        // ============================================================================
        // STUDENT ENROLLMENTS STREAM PROCESSING
        // ============================================================================
        log.info("Processing StudentEnrollment stream from topic: {}", enrollmentTopic);
        
        // STAGE 1a: StudentEnrollment deserialization validation - separate valid and invalid streams
        KStream<byte[], byte[]> inputEnrollments = builder.stream(enrollmentTopic, Consumed.with(bytesSerde, bytesSerde));
        inputEnrollments.peek((key, value) -> log.info("Consume StudentEnrollment Key: {} value: {}", key, value));
        
        // Predicate for valid enrollment records
        java.util.function.Predicate<? super byte[]> isValidEnrollment = v -> {
            try {
                if (v == null) {
                    return false;
                } else {
                    serdesFactory.studentEnrollmentSpecificAvroSerde().deserializer().deserialize(enrollmentTopic, v);
                    return true;
                }
            } catch (SerializationException ignored) {
                log.error("StudentEnrollment deserialization error: {}", ignored.getMessage());
                return false;
            }
        };
        
        // STAGE 1b: Branch into valid and invalid streams
        KStream<byte[], byte[]> validEnrollmentStream = inputEnrollments.filter((k, v) -> isValidEnrollment.test(v));
        KStream<byte[], byte[]> invalidEnrollmentStream = inputEnrollments.filterNot((k, v) -> isValidEnrollment.test(v));

        // Send invalid records to DLQ
        invalidEnrollmentStream.to("enrollments-dlq", Produced.with(bytesSerde, bytesSerde));

        // STAGE 2: Deserialize valid enrollment records to Avro
        final KStream<String, StudentEnrollment> enrollmentAvroStream = validEnrollmentStream
                .map((key, value) -> {
                    String enrollmentId = null;
                    StudentEnrollment enrollment = null;
                    try {
                        enrollment = serdesFactory.studentEnrollmentSpecificAvroSerde().deserializer().deserialize(enrollmentTopic, value);
                        enrollmentId = enrollment.getEnrollmentId() != null ? enrollment.getEnrollmentId().toString() : new String(key);
                    } catch (SerializationException e) {
                        log.error("Failed to deserialize StudentEnrollment: {}", e.getMessage());
                        enrollmentId = new String(key);
                    }
                    return KeyValue.pair(enrollmentId, enrollment);
                });

        // STAGE 3: Create StudentEnrollment KTable with reduce logic (latest by timestamp)
        studentEnrollmentKTable = enrollmentAvroStream
                .selectKey((key, value) -> value != null ? value.getEnrollmentId().toString() : key)
                .groupByKey(Grouped.with(stringSerde, serdesFactory.studentEnrollmentSpecificAvroSerde()))
                .reduce((enrollment1, enrollment2) -> {
                    if (enrollment1 == null) return enrollment2;
                    if (enrollment2 == null) return enrollment1;
                    
                    long ts1 = enrollment1.getUpdatedAt() != null ? enrollment1.getUpdatedAt() : 0L;
                    long ts2 = enrollment2.getUpdatedAt() != null ? enrollment2.getUpdatedAt() : 0L;
                    log.info("StudentEnrollment reduce - Enrollment1 timestamp: {} Enrollment2 timestamp: {}", ts1, ts2);
                    return (ts1 > ts2) ? enrollment1 : enrollment2;
                })
                .toStream()
                .toTable(Materialized.<String, StudentEnrollment, KeyValueStore<Bytes, byte[]>>
                        as("ENROLLMENTS-MV")
                        .withKeySerde(stringSerde)
                        .withValueSerde(serdesFactory.studentEnrollmentSpecificAvroSerde()));
    }
}
