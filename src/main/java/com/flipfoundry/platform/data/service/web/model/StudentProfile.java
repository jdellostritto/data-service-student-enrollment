package com.flipfoundry.platform.data.service.web.model;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * StudentProfile model represents the joined data of a Student with their Enrollment.
 */
@Table(value = "student_profiles")
public class StudentProfile {

	@PrimaryKey
	@Column("student_id")
	private String studentId;

	@Column("state_student_id")
	private String stateStudentId;

	@Column("first_name")
	private String firstName;

	@Column("last_name")
	private String lastName;

	@Column("date_of_birth")
	private String dateOfBirth;

	@Column("grade_level")
	private String gradeLevel;

	@Column("enrollment_id")
	private String enrollmentId;

	@Column("school_id")
	private String schoolId;

	@Column("district_id")
	private String districtId;

	@Column("school_year")
	private String schoolYear;

	@Column("entry_date")
	private String entryDate;

	@Column("exit_date")
	private String exitDate;

	@Column("enrollment_status")
	private String enrollmentStatus;

	@Column("student_updated_at")
	private Long studentUpdatedAt;

	@Column("enrollment_updated_at")
	private Long enrollmentUpdatedAt;

	// Constructors
	public StudentProfile() {}

	public StudentProfile(String studentId, String stateStudentId, String firstName, String lastName,
			String dateOfBirth, String gradeLevel, String enrollmentId, String schoolId, String districtId,
			String schoolYear, String entryDate, String exitDate, String enrollmentStatus,
			Long studentUpdatedAt, Long enrollmentUpdatedAt) {
		this.studentId = studentId;
		this.stateStudentId = stateStudentId;
		this.firstName = firstName;
		this.lastName = lastName;
		this.dateOfBirth = dateOfBirth;
		this.gradeLevel = gradeLevel;
		this.enrollmentId = enrollmentId;
		this.schoolId = schoolId;
		this.districtId = districtId;
		this.schoolYear = schoolYear;
		this.entryDate = entryDate;
		this.exitDate = exitDate;
		this.enrollmentStatus = enrollmentStatus;
		this.studentUpdatedAt = studentUpdatedAt;
		this.enrollmentUpdatedAt = enrollmentUpdatedAt;
	}

	// Getters and Setters
	public String getStudentId() {
		return studentId;
	}

	public void setStudentId(String studentId) {
		this.studentId = studentId;
	}

	public String getStateStudentId() {
		return stateStudentId;
	}

	public void setStateStudentId(String stateStudentId) {
		this.stateStudentId = stateStudentId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getDateOfBirth() {
		return dateOfBirth;
	}

	public void setDateOfBirth(String dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}

	public String getGradeLevel() {
		return gradeLevel;
	}

	public void setGradeLevel(String gradeLevel) {
		this.gradeLevel = gradeLevel;
	}

	public String getEnrollmentId() {
		return enrollmentId;
	}

	public void setEnrollmentId(String enrollmentId) {
		this.enrollmentId = enrollmentId;
	}

	public String getSchoolId() {
		return schoolId;
	}

	public void setSchoolId(String schoolId) {
		this.schoolId = schoolId;
	}

	public String getDistrictId() {
		return districtId;
	}

	public void setDistrictId(String districtId) {
		this.districtId = districtId;
	}

	public String getSchoolYear() {
		return schoolYear;
	}

	public void setSchoolYear(String schoolYear) {
		this.schoolYear = schoolYear;
	}

	public String getEntryDate() {
		return entryDate;
	}

	public void setEntryDate(String entryDate) {
		this.entryDate = entryDate;
	}

	public String getExitDate() {
		return exitDate;
	}

	public void setExitDate(String exitDate) {
		this.exitDate = exitDate;
	}

	public String getEnrollmentStatus() {
		return enrollmentStatus;
	}

	public void setEnrollmentStatus(String enrollmentStatus) {
		this.enrollmentStatus = enrollmentStatus;
	}

	public Long getStudentUpdatedAt() {
		return studentUpdatedAt;
	}

	public void setStudentUpdatedAt(Long studentUpdatedAt) {
		this.studentUpdatedAt = studentUpdatedAt;
	}

	public Long getEnrollmentUpdatedAt() {
		return enrollmentUpdatedAt;
	}

	public void setEnrollmentUpdatedAt(Long enrollmentUpdatedAt) {
		this.enrollmentUpdatedAt = enrollmentUpdatedAt;
	}
}
