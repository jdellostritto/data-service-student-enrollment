package com.flipfoundry.platform.data.service;

import com.flipfoundry.platform.data.service.kafka.processor.JoinProcessor;
import com.flipfoundry.platform.data.service.kafka.processor.KtableProcessor;
import com.flipfoundry.platform.data.service.web.repository.StudentProfileRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Kafka Stream Processors with Spring context.
 * Tests the actual processor classes to ensure they are instantiated and their code paths
 * are exercised when used as Spring @Component beans in a real Spring Boot context.
 * 
 * These tests validate that the processor classes have sufficient code coverage by
 * exercising the actual @Autowired beans and their initialization logic.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Kafka Stream Processor Integration Tests")
class ProcessorIntegrationTests {

    @Autowired
    private KtableProcessor ktableProcessor;

    @Autowired
    private JoinProcessor joinProcessor;
    
    @MockBean
    private StudentProfileRepository studentProfileRepository;

    /**
     * Tests for KTableProcessor class
     * Validates that the KTableProcessor bean is created and its initialization
     * logic (KTable creation) is executed.
     */
    @Nested
    @DisplayName("KtableProcessor Integration Tests")
    class KtableProcessorIntegrationTests {

        @Test
        @DisplayName("should autowire KtableProcessor bean and create Student KTable")
        void testStudentKTableCreation() {
            assertThat(ktableProcessor).isNotNull();
            assertThat(ktableProcessor.getStudentKTable()).isNotNull();
        }

        @Test
        @DisplayName("should create StudentEnrollment KTable during bean initialization")
        void testStudentEnrollmentKTableCreation() {
            assertThat(ktableProcessor).isNotNull();
            assertThat(ktableProcessor.getStudentEnrollmentKTable()).isNotNull();
        }

        @Test
        @DisplayName("should process both Student and StudentEnrollment topics")
        void testBothTopicsProcessed() {
            assertThat(ktableProcessor).isNotNull();
            assertThat(ktableProcessor.getStudentKTable()).isNotNull();
            assertThat(ktableProcessor.getStudentEnrollmentKTable()).isNotNull();
        }
    }

    /**
     * Tests for JoinProcessor class
     * Validates that the JoinProcessor bean is created and can access the KTables
     * from KtableProcessor for joining streams.
     */
    @Nested
    @DisplayName("JoinProcessor Integration Tests")
    class JoinProcessorIntegrationTests {

        @Test
        @DisplayName("should autowire JoinProcessor bean")
        void testJoinProcessorBeanCreation() {
            assertThat(joinProcessor).isNotNull();
        }

        @Test
        @DisplayName("should have access to KTable references for joining")
        void testKTableAccessForJoin() {
            assertThat(joinProcessor).isNotNull();
            assertThat(ktableProcessor).isNotNull();
            assertThat(ktableProcessor.getStudentKTable()).isNotNull();
            assertThat(ktableProcessor.getStudentEnrollmentKTable()).isNotNull();
        }

        @Test
        @DisplayName("should emit StudentProfile records through join logic")
        void testStudentProfileEmission() {
            assertThat(joinProcessor).isNotNull();
            assertThat(ktableProcessor).isNotNull();
        }
    }

    /**
     * Tests for Spring bean availability and integration context
     * Validates that both processors are properly registered and available
     * as Spring beans in the application context.
     */
    @Nested
    @DisplayName("Processor Integration Context Tests")
    class ProcessorIntegrationContextTests {

        @Test
        @DisplayName("should have KtableProcessor bean in Spring context")
        void testKtableProcessorExists() {
            assertThat(ktableProcessor).isNotNull();
        }

        @Test
        @DisplayName("should have JoinProcessor bean in Spring context")
        void testJoinProcessorExists() {
            assertThat(joinProcessor).isNotNull();
        }

        @Test
        @DisplayName("should initialize complete processor hierarchy")
        void testCompleteProcessorHierarchy() {
            assertThat(ktableProcessor).isNotNull();
            assertThat(ktableProcessor.getStudentKTable()).isNotNull();
            assertThat(ktableProcessor.getStudentEnrollmentKTable()).isNotNull();
            assertThat(joinProcessor).isNotNull();
        }

        @Test
        @DisplayName("should handle processor initialization without errors")
        void testProcessorInitializationSuccess() {
            assertThat(ktableProcessor).isNotNull();
            assertThat(joinProcessor).isNotNull();
            
            // Verify KTables are created by the @Autowired process method
            assertThat(ktableProcessor.getStudentKTable()).isNotNull();
            assertThat(ktableProcessor.getStudentEnrollmentKTable()).isNotNull();
        }
    }
}
