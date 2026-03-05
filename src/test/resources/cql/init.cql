-- Create keyspace for student enrollment data service
CREATE KEYSPACE IF NOT EXISTS student_enrollment WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE student_enrollment;

-- Students table
CREATE TABLE IF NOT EXISTS students(
    student_id text,
    state_student_id text,
    first_name text,
    last_name text,
    date_of_birth text,
    grade_level text,
    updated_at bigint,
    PRIMARY KEY(student_id)
);

-- Student Enrollments table
CREATE TABLE IF NOT EXISTS student_enrollments(
    enrollment_id text,
    student_id text,
    school_id text,
    district_id text,
    school_year text,
    entry_date text,
    exit_date text,
    enrollment_status text,
    updated_at bigint,
    PRIMARY KEY(enrollment_id)
);

-- Student Profiles table (join result)
CREATE TABLE IF NOT EXISTS student_profiles(
    student_id text,
    state_student_id text,
    first_name text,
    last_name text,
    date_of_birth text,
    grade_level text,
    student_updated_at bigint,
    enrollment_id text,
    school_id text,
    district_id text,
    school_year text,
    entry_date text,
    exit_date text,
    enrollment_status text,
    enrollment_updated_at bigint,
    PRIMARY KEY(student_id)
);
