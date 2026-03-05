package com.flipfoundry.platform.data.service.kafka.utils;

import com.flipfoundry.platform.data.service.avro.Student;
import com.flipfoundry.platform.data.service.avro.StudentEnrollment;
import com.flipfoundry.platform.data.service.avro.StudentEnrollmentKey;
import com.flipfoundry.platform.data.service.avro.StudentKey;
import com.flipfoundry.platform.data.service.avro.StudentProfile;
import com.flipfoundry.platform.data.service.avro.StudentProfileKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Factory for creating Kafka Serdes for Student, StudentEnrollment, and StudentProfile Avro types.
 */
@Component
public class SerdesFactory {

    private static String srUrl = "http://localhost:8081";

    @org.springframework.beans.factory.annotation.Value("${spring.kafka.properties.schema.registry.url}")
    public void setSrUrl(String url) {
        srUrl = url;
    }

    /**
     * Student VALUE
     * SpecificAvroSerde
     * @return specificAvroSerde
     */
    static public final SpecificAvroSerde<Student> studentSpecificAvroSerde() {
        SpecificAvroSerde<Student> studentSpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        studentSpecificAvroSerde.configure(config, false);
        return studentSpecificAvroSerde;
    }

    /**
     * StudentEnrollment VALUE
     * SpecificAvroSerde
     * @return specificAvroSerde
     */
    static public final SpecificAvroSerde<StudentEnrollment> studentEnrollmentSpecificAvroSerde() {
        SpecificAvroSerde<StudentEnrollment> studentEnrollmentSpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        studentEnrollmentSpecificAvroSerde.configure(config, false);
        return studentEnrollmentSpecificAvroSerde;
    }

    /**
     * StudentProfile
     * SpecificAvroSerde
     * @return specificAvroSerde
     */
    static public final SpecificAvroSerde<StudentProfile> studentProfileSpecificAvroSerde() {
        SpecificAvroSerde<StudentProfile> studentProfileSpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        studentProfileSpecificAvroSerde.configure(config, false);
        return studentProfileSpecificAvroSerde;
    }

    /**
     * StudentKey
     * SpecificAvroSerde
     * @return specificAvroSerde
     */
    static public final SpecificAvroSerde<StudentKey> studentKeySpecificAvroSerde() {
        SpecificAvroSerde<StudentKey> studentKeySpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        studentKeySpecificAvroSerde.configure(config, true);
        return studentKeySpecificAvroSerde;
    }

    /**
     * StudentEnrollmentKey
     * SpecificAvroSerde
     * @return specificAvroSerde
     */
    static public final SpecificAvroSerde<StudentEnrollmentKey> studentEnrollmentKeySpecificAvroSerde() {
        SpecificAvroSerde<StudentEnrollmentKey> studentEnrollmentKeySpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        studentEnrollmentKeySpecificAvroSerde.configure(config, true);
        return studentEnrollmentKeySpecificAvroSerde;
    }

    /**
     * StudentProfileKey
     * SpecificAvroSerde
     * @return specificAvroSerde
     */
    static public final SpecificAvroSerde<StudentProfileKey> studentProfileKeySpecificAvroSerde() {
        SpecificAvroSerde<StudentProfileKey> studentProfileKeySpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        studentProfileKeySpecificAvroSerde.configure(config, true);
        return studentProfileKeySpecificAvroSerde;
    }


}
