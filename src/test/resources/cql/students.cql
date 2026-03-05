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
