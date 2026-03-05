-- Cassandra initialization script for integration tests
-- Creates keyspace and tables for StudentProfile storage

CREATE KEYSPACE IF NOT EXISTS flipfoundry
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flipfoundry;

CREATE TABLE IF NOT EXISTS student_profiles (
    student_id text PRIMARY KEY,
    state_student_id text,
    first_name text,
    last_name text,
    date_of_birth text,
    grade_level text,
    enrollment_id text,
    school_id text,
    district_id text,
    school_year text,
    entry_date text,
    exit_date text,
    enrollment_status text,
    student_updated_at bigint,
    enrollment_updated_at bigint,
    created_at timestamp,
    updated_at timestamp
);
