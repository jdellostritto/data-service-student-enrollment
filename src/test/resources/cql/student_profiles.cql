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
