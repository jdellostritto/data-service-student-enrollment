package com.flipfoundry.platform.data.service.kafka.builders;

import com.flipfoundry.platform.data.service.avro.Student;
import com.flipfoundry.platform.data.service.avro.StudentEnrollment;
import com.flipfoundry.platform.data.service.avro.StudentProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit Tests for StudentProfileBuilder.
 * Tests the builder logic for creating StudentProfile from Student and StudentEnrollment data.
 * 
 * The StudentProfile requires data from both Student and StudentEnrollment to satisfy
 * all required fields, so tests always provide both sources.
 */
@DisplayName("StudentProfileBuilder Unit Tests")
class StudentProfileBuilderTests {

    private StudentProfileBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new StudentProfileBuilder();
    }

    /**
     * Helper method to create a complete Student with all required and optional fields
     */
    private Student createCompleteStudent(String studentId, String firstName, String lastName) {
        return Student.newBuilder()
                .setStudentId(studentId)
                .setStateStudentId("STATE-" + studentId)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setDateOfBirth("2010-05-15")
                .setGradeLevel("10")
                .setUpdatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Helper method to create a complete StudentEnrollment with all required and optional fields
     */
    private StudentEnrollment createCompleteEnrollment(String studentId) {
        return StudentEnrollment.newBuilder()
                .setStudentId(studentId)
                .setEnrollmentId("ENROLL-" + studentId)
                .setSchoolId("SCH-001")
                .setDistrictId("DIST-001")
                .setSchoolYear("2025-2026")
                .setEntryDate("2025-08-01")
                .setExitDate(null)
                .setEnrollmentStatus("ACTIVE")
                .setUpdatedAt(System.currentTimeMillis())
                .build();
    }

    @Nested
    @DisplayName("Builder Creation Tests")
    class BuilderCreationTests {

        @Test
        @DisplayName("should create StudentProfileBuilder instance")
        void shouldCreateBuilder() {
            assertThat(builder).isNotNull();
        }

        @Test
        @DisplayName("should instantiate multiple independent builders")
        void shouldCreateMultipleBuilders() {
            StudentProfileBuilder builder1 = new StudentProfileBuilder();
            StudentProfileBuilder builder2 = new StudentProfileBuilder();

            assertThat(builder1).isNotNull();
            assertThat(builder2).isNotNull();
            assertThat(builder1).isNotSameAs(builder2);
        }
    }

    @Nested
    @DisplayName("Builder with Both Student and Enrollment Tests")
    class BuilderWithBothDataTests {

        @Test
        @DisplayName("should build StudentProfile from Student and StudentEnrollment")
        void shouldCombineStudentAndEnrollment() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = createCompleteStudent("STU-001", "John", "Doe");
            StudentEnrollment enrollment = createCompleteEnrollment("STU-001");

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert
            assertThat(profile).isNotNull();
            assertThat(profile.getStudentId()).isEqualTo("STU-001");
            assertThat(profile.getFirstName()).isEqualTo("John");
            assertThat(profile.getLastName()).isEqualTo("Doe");
            assertThat(profile.getStateStudentId()).isEqualTo("STATE-STU-001");
            assertThat(profile.getEnrollmentId()).isEqualTo("ENROLL-STU-001");
            assertThat(profile.getSchoolId()).isEqualTo("SCH-001");
            assertThat(profile.getEnrollmentStatus()).isEqualTo("ACTIVE");
        }

        @Test
        @DisplayName("should include Student optional fields in profile")
        void shouldIncludeStudentOptionalFields() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = createCompleteStudent("STU-002", "Jane", "Smith");
            StudentEnrollment enrollment = createCompleteEnrollment("STU-002");

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert
            assertThat(profile.getDateOfBirth()).isEqualTo("2010-05-15");
            assertThat(profile.getGradeLevel()).isEqualTo("10");
            assertThat(profile.getStudentUpdatedAt()).isNotNull();
        }

        @Test
        @DisplayName("should include Enrollment optional fields in profile")
        void shouldIncludeEnrollmentOptionalFields() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = createCompleteStudent("STU-003", "Alice", "Wilson");
            StudentEnrollment enrollment = createCompleteEnrollment("STU-003");

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert
            assertThat(profile.getEntryDate()).isEqualTo("2025-08-01");
            assertThat(profile.getEnrollmentUpdatedAt()).isNotNull();
        }

        @Test
        @DisplayName("should handle enrollment with exit date")
        void shouldHandleExitDate() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = createCompleteStudent("STU-004", "Bob", "Johnson");
            StudentEnrollment enrollment = StudentEnrollment.newBuilder()
                    .setStudentId("STU-004")
                    .setEnrollmentId("ENROLL-STU-004")
                    .setSchoolId("SCH-001")
                    .setDistrictId("DIST-001")
                    .setSchoolYear("2024-2025")
                    .setEntryDate("2024-08-01")
                    .setExitDate("2025-06-30")
                    .setEnrollmentStatus("COMPLETED")
                    .setUpdatedAt(System.currentTimeMillis())
                    .build();

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert
            assertThat(profile.getExitDate()).isEqualTo("2025-06-30");
            assertThat(profile.getEnrollmentStatus()).isEqualTo("COMPLETED");
        }
    }

    @Nested
    @DisplayName("Builder Multiple Call Tests")
    class BuilderMultipleCallTests {

        @Test
        @DisplayName("should support building multiple profiles sequentially")
        void shouldBuildMultipleProfiles() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student1 = createCompleteStudent("STU-101", "Alice", "Brown");
            StudentEnrollment enrollment1 = createCompleteEnrollment("STU-101");

            Student student2 = createCompleteStudent("STU-102", "Bob", "Green");
            StudentEnrollment enrollment2 = createCompleteEnrollment("STU-102");

            // Act
            StudentProfile profile1 = builder.build(student1, enrollment1);
            StudentProfile profile2 = builder.build(student2, enrollment2);

            // Assert
            assertThat(profile1).isNotNull();
            assertThat(profile2).isNotNull();
            assertThat(profile1.getStudentId()).isEqualTo("STU-101");
            assertThat(profile2.getStudentId()).isEqualTo("STU-102");
            assertThat(profile1.getFirstName()).isEqualTo("Alice");
            assertThat(profile2.getFirstName()).isEqualTo("Bob");
        }

        @Test
        @DisplayName("should build different enrollments for same student")
        void shouldBuildDifferentEnrollmentsForStudent() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = createCompleteStudent("STU-200", "Charlie", "Davis");

            StudentEnrollment enrollment1 = StudentEnrollment.newBuilder()
                    .setStudentId("STU-200")
                    .setEnrollmentId("ENROLL-2024")
                    .setSchoolId("SCH-001")
                    .setDistrictId("DIST-001")
                    .setSchoolYear("2024-2025")
                    .setEntryDate("2024-08-01")
                    .setEnrollmentStatus("COMPLETED")
                    .setUpdatedAt(System.currentTimeMillis())
                    .build();

            StudentEnrollment enrollment2 = StudentEnrollment.newBuilder()
                    .setStudentId("STU-200")
                    .setEnrollmentId("ENROLL-2025")
                    .setSchoolId("SCH-001")
                    .setDistrictId("DIST-001")
                    .setSchoolYear("2025-2026")
                    .setEntryDate("2025-08-01")
                    .setEnrollmentStatus("ACTIVE")
                    .setUpdatedAt(System.currentTimeMillis())
                    .build();

            // Act
            StudentProfile profile1 = builder.build(student, enrollment1);
            StudentProfile profile2 = builder.build(student, enrollment2);

            // Assert
            assertThat(profile1.getStudentId()).isEqualTo(profile2.getStudentId());
            assertThat(profile1.getFirstName()).isEqualTo(profile2.getFirstName());
            assertThat(profile1.getEnrollmentId()).isEqualTo("ENROLL-2024");
            assertThat(profile2.getEnrollmentId()).isEqualTo("ENROLL-2025");
            assertThat(profile1.getSchoolYear()).isEqualTo("2024-2025");
            assertThat(profile2.getSchoolYear()).isEqualTo("2025-2026");
        }
    }

    @Nested
    @DisplayName("Builder Data Merging Tests")
    class BuilderDataMergingTests {

        @Test
        @DisplayName("should merge fields from both Student and Enrollment")
        void shouldMergeDataFromBothSources() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = createCompleteStudent("STU-300", "Emma", "Frank");
            StudentEnrollment enrollment = createCompleteEnrollment("STU-300");

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert - Verify fields from Student
            assertThat(profile.getFirstName()).isEqualTo("Emma"); // From Student
            assertThat(profile.getLastName()).isEqualTo("Frank"); // From Student
            assertThat(profile.getGradeLevel()).isEqualTo("10"); // From Student

            // Verify fields from Enrollment
            assertThat(profile.getEnrollmentId()).isEqualTo("ENROLL-STU-300"); // From Enrollment
            assertThat(profile.getSchoolId()).isEqualTo("SCH-001"); // From Enrollment
            assertThat(profile.getSchoolYear()).isEqualTo("2025-2026"); // From Enrollment
        }

        @Test
        @DisplayName("should populate both updated_at timestamps")
        void shouldIncludeTimestamps() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            long beforeTime = System.currentTimeMillis();
            Student student = createCompleteStudent("STU-400", "Frank", "Garcia");
            StudentEnrollment enrollment = createCompleteEnrollment("STU-400");
            long afterTime = System.currentTimeMillis();

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert
            assertThat(profile.getStudentUpdatedAt())
                    .isGreaterThanOrEqualTo(beforeTime)
                    .isLessThanOrEqualTo(afterTime);
            assertThat(profile.getEnrollmentUpdatedAt())
                    .isGreaterThanOrEqualTo(beforeTime)
                    .isLessThanOrEqualTo(afterTime);
        }
    }

    @Nested
    @DisplayName("Builder Field Handling Tests")
    class BuilderFieldHandlingTests {

        @Test
        @DisplayName("should handle null optional Student fields")
        void shouldHandleNullStudentOptionalFields() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = Student.newBuilder()
                    .setStudentId("STU-500")
                    .setFirstName("Grace")
                    .setLastName("Harris")
                    .setStateStudentId(null)
                    .setDateOfBirth(null)
                    .setGradeLevel(null)
                    .setUpdatedAt(System.currentTimeMillis())
                    .build();

            StudentEnrollment enrollment = createCompleteEnrollment("STU-500");

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert
            assertThat(profile).isNotNull();
            assertThat(profile.getFirstName()).isEqualTo("Grace");
            assertThat(profile.getStateStudentId()).isNull();
            assertThat(profile.getDateOfBirth()).isNull();
            assertThat(profile.getGradeLevel()).isNull();
        }

        @Test
        @DisplayName("should handle null optional Enrollment fields")
        void shouldHandleNullEnrollmentOptionalFields() throws InvocationTargetException, IllegalAccessException {
            // Arrange
            Student student = createCompleteStudent("STU-600", "Henry", "Jackson");

            StudentEnrollment enrollment = StudentEnrollment.newBuilder()
                    .setStudentId("STU-600")
                    .setEnrollmentId("ENROLL-STU-600")
                    .setSchoolId("SCH-001")
                    .setDistrictId("DIST-001")
                    .setSchoolYear("2025-2026")
                    .setEntryDate(null)
                    .setExitDate(null)
                    .setEnrollmentStatus(null)
                    .setUpdatedAt(System.currentTimeMillis())
                    .build();

            // Act
            StudentProfile profile = builder.build(student, enrollment);

            // Assert
            assertThat(profile).isNotNull();
            assertThat(profile.getEntryDate()).isNull();
            assertThat(profile.getExitDate()).isNull();
            assertThat(profile.getEnrollmentStatus()).isNull();
        }
    }
}
