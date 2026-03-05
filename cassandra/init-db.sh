#!/bin/bash
set -e

echo "Waiting for Cassandra to be ready..."
for i in {1..30}; do
  if cqlsh -u cassandra -p cassandra $CASSANDRA_HOST $CASSANDRA_PORT -e "DESCRIBE KEYSPACES;" 2>/dev/null; then
    echo "Cassandra is ready!"
    break
  fi
  echo "Attempt $i/30: Cassandra not ready yet, sleeping..."
  sleep 2
done

echo "Creating keyspace and tables..."
cqlsh -u cassandra -p cassandra $CASSANDRA_HOST $CASSANDRA_PORT << EOF
CREATE KEYSPACE IF NOT EXISTS student_enrollment WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE student_enrollment;

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
EOF

echo "Keyspace and tables created successfully!"
