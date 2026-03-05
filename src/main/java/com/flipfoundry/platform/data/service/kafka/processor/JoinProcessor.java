package com.flipfoundry.platform.data.service.kafka.processor;

import com.flipfoundry.platform.data.service.avro.Student;
import com.flipfoundry.platform.data.service.avro.StudentEnrollment;
import com.flipfoundry.platform.data.service.kafka.builders.StudentProfileBuilder;
import com.flipfoundry.platform.data.service.kafka.utils.SerdesFactory;
import com.flipfoundry.platform.data.service.web.repository.StudentProfileRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

/**
 * Join Processor for Student and StudentEnrollment streams.
 * Joins Student KTable with StudentEnrollment KTable on student_id
 * and outputs StudentProfile records to both Kafka topic and Cassandra.
 */
@Slf4j
@EnableKafka
@EnableKafkaStreams
@Component
@ConfigurationProperties
public class JoinProcessor {

    @Autowired
    SerdesFactory serdesFactory;

    @Autowired
    KtableProcessor ktableProcessor;

    @Autowired
    StudentProfileBuilder studentProfileBuilder;

    @Autowired
    StudentProfileRepository studentProfileRepository;

    /**
     * Process method that performs the join
     * Joins StudentEnrollment (left) with Student (right) on student_id
     * Output is StudentProfile records written to materialized view and Cassandra
     */
    @Autowired
    public void process(StreamsBuilder builder) {
        log.info("Starting Student-StudentEnrollment join processing");

        // Get the KTables from KtableProcessor
        KTable<String, StudentEnrollment> enrollmentKTable = ktableProcessor.getStudentEnrollmentKTable();
        KTable<String, Student> studentKTable = ktableProcessor.getStudentKTable();

        // Perform the join: StudentEnrollment (left) joined with Student (right) on student_id
        // The result is a StudentProfile Avro object
        final KTable<String, com.flipfoundry.platform.data.service.avro.StudentProfile> studentProfileAvroKTable =
                enrollmentKTable.join(
                        studentKTable,
                        StudentEnrollment::getStudentId,  // Extract student_id from enrollment
                        (enrollment, student) -> {
                            // Build StudentProfile from enrollment and student
                            com.flipfoundry.platform.data.service.avro.StudentProfile profile = null;
                            try {
                                profile = studentProfileBuilder.build(student, enrollment);
                            } catch (Exception e) {
                                log.error("Error building StudentProfile: {}", e.getMessage(), e);
                                // Return null or a default profile
                                profile = new com.flipfoundry.platform.data.service.avro.StudentProfile();
                            }
                            
                            if (student != null && enrollment != null) {
                                log.info("JOINED RESULT - Student: {} Enrollment: {}", 
                                        student.getStudentId(), enrollment.getEnrollmentId());
                            }
                            
                            // Save to Cassandra
                            try {
                                if (student != null && studentProfileRepository != null) {
                                    com.flipfoundry.platform.data.service.web.model.StudentProfile spEntity = 
                                            new com.flipfoundry.platform.data.service.web.model.StudentProfile();
                                    spEntity.setStudentId(student.getStudentId().toString());
                                    spEntity.setStateStudentId(student.getStateStudentId() != null ? student.getStateStudentId().toString() : null);
                                    spEntity.setFirstName(student.getFirstName() != null ? student.getFirstName().toString() : null);
                                    spEntity.setLastName(student.getLastName() != null ? student.getLastName().toString() : null);
                                    spEntity.setDateOfBirth(student.getDateOfBirth() != null ? student.getDateOfBirth().toString() : null);
                                    spEntity.setGradeLevel(student.getGradeLevel() != null ? student.getGradeLevel().toString() : null);
                                    spEntity.setStudentUpdatedAt(student.getUpdatedAt());
                                    
                                    if (enrollment != null) {
                                        spEntity.setEnrollmentId(enrollment.getEnrollmentId().toString());
                                        spEntity.setSchoolId(enrollment.getSchoolId() != null ? enrollment.getSchoolId().toString() : null);
                                        spEntity.setDistrictId(enrollment.getDistrictId() != null ? enrollment.getDistrictId().toString() : null);
                                        spEntity.setSchoolYear(enrollment.getSchoolYear() != null ? enrollment.getSchoolYear().toString() : null);
                                        spEntity.setEntryDate(enrollment.getEntryDate() != null ? enrollment.getEntryDate().toString() : null);
                                        spEntity.setExitDate(enrollment.getExitDate() != null ? enrollment.getExitDate().toString() : null);
                                        spEntity.setEnrollmentStatus(enrollment.getEnrollmentStatus() != null ? enrollment.getEnrollmentStatus().toString() : null);
                                        spEntity.setEnrollmentUpdatedAt(enrollment.getUpdatedAt());
                                    }
                                    
                                    log.debug("Saving StudentProfile to Cassandra - StudentId: {}, EnrollmentId: {}", 
                                        spEntity.getStudentId(), spEntity.getEnrollmentId());
                                    studentProfileRepository.save(spEntity);
                                    log.debug("Successfully saved StudentProfile - StudentId: {}", spEntity.getStudentId());
                                } else {
                                    log.warn("Cannot save StudentProfile: student={}, repository={}", student != null, studentProfileRepository != null);
                                }
                            } catch (Exception e) {
                                log.error("Error saving StudentProfile to Cassandra - StudentId: {}: {} - Cause: {}", 
                                    student != null ? student.getStudentId() : "null", e.getMessage(), e.getCause(), e);
                                e.printStackTrace();
                            }
                            
                            return profile;
                        },
                        // Materialize to state store
                        Materialized.<String, com.flipfoundry.platform.data.service.avro.StudentProfile, KeyValueStore<Bytes, byte[]>>
                                as("STUDENT-PROFILES-MV")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdesFactory.studentProfileSpecificAvroSerde())
                );

        // Optionally output StudentProfile records to an output topic
        studentProfileAvroKTable.toStream()
                .peek((key, value) -> log.info("Outputting StudentProfile - Key: {} Value: {}", key, value))
                .to("student-profiles", Produced.with(Serdes.String(), serdesFactory.studentProfileSpecificAvroSerde()));
    }
}
