package com.flipfoundry.platform.data.service.config;

import com.flipfoundry.platform.data.service.web.repository.StudentProfileRepository;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import static org.mockito.Mockito.mock;

/**
 * Test configuration that mocks external dependencies needed for processor integration tests.
 * This allows us to test processor logic without requiring actual Cassandra/database setup.
 */
@TestConfiguration
public class ProcessorTestConfiguration {

    /**
     * Mock the StudentProfileRepository to prevent Spring from trying to connect to Cassandra
     * during test context initialization.
     */
    @Bean
    @Primary
    public StudentProfileRepository mockStudentProfileRepository() {
        return mock(StudentProfileRepository.class);
    }
}
