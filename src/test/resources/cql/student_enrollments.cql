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
