package com.flipfoundry.platform.data.service.kafka.builders;

import com.flipfoundry.platform.data.service.avro.Student;
import com.flipfoundry.platform.data.service.avro.StudentEnrollment;
import com.flipfoundry.platform.data.service.avro.StudentProfile;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;

/**
 * Builder for StudentProfile records from Student and StudentEnrollment Avro objects.
 */
@Component
public class StudentProfileBuilder {

	public StudentProfile build(Student student, StudentEnrollment enrollment)
			throws InvocationTargetException, IllegalAccessException {

		StudentProfile.Builder builder = StudentProfile.newBuilder();

		if (student != null) {
			builder.setStudentId(student.getStudentId());
			builder.setStateStudentId(student.getStateStudentId());
			builder.setFirstName(student.getFirstName());
			builder.setLastName(student.getLastName());
			builder.setDateOfBirth(student.getDateOfBirth());
			builder.setGradeLevel(student.getGradeLevel());
			builder.setStudentUpdatedAt(student.getUpdatedAt());
		}

		if (enrollment != null) {
			builder.setEnrollmentId(enrollment.getEnrollmentId());
			builder.setStudentId(enrollment.getStudentId());
			builder.setSchoolId(enrollment.getSchoolId());
			builder.setDistrictId(enrollment.getDistrictId());
			builder.setSchoolYear(enrollment.getSchoolYear());
			builder.setEntryDate(enrollment.getEntryDate());
			builder.setExitDate(enrollment.getExitDate());
			builder.setEnrollmentStatus(enrollment.getEnrollmentStatus());
			builder.setEnrollmentUpdatedAt(enrollment.getUpdatedAt());
		}

		return builder.build();
	}
}
