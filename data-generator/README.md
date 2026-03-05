# Data Generator for Student Enrollment Service

Generates realistic student and enrollment data and publishes to Kafka topics for end-to-end testing of the Student Enrollment Data Service.

## Prerequisites

### 1. Start the Full Stack

```bash
cd ..
make run-stack
```

This starts:

- Cassandra (port 9042)
- Kafka/Confluent (Broker: 9092, Schema Registry: 8081, Control Center: 9021)
- Application (port 8700)

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

Or use a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

### Generate 25 Student/Enrollment Pairs (Default)

```bash
python data_generator.py
```

### Generate Custom Number of Records

```bash
# 100 pairs
python data_generator.py --records 100

# 50 only with 2-second interval
python data_generator.py --records 50 --interval 2
```

### Generate Students Only (No Enrollments)

```bash
python data_generator.py --records 50 --students-only
```

### Mock Run (No Kafka Connection)

Useful for testing the script without a running Kafka instance:

```bash
python data_generator.py --mock --records 10
```

### Full Command Options

```bash
python data_generator.py --help

# Examples:
python data_generator.py -r 100               # 100 pairs
python data_generator.py -r 50 -i 2           # 50 pairs, 2-second interval
python data_generator.py -r 75 -s             # 75 students only
python data_generator.py -m -r 20             # Mock mode: 20 pairs
python data_generator.py -b kafka:9092 -r 30 # Custom Kafka server
```

## Overall End-to-End Flow

### 1. **Terminal 1: Start the Full Stack**

```bash
make run-stack
# Waits for services to be healthy
```

### 2. **Terminal 2: Generate Data**

```bash
# Install dependencies (first time only)
pip install -r requirements.txt

# Run data generator
python data_generator.py --records 50 --interval 1
```

### 3. **Terminal 3: Monitor Results**

#### Option A: Check Cassandra

```bash
docker exec -it cass cqlsh -u cassandra -p cassandra
USE student_enrollment;
SELECT * FROM student_profiles;
```

#### Option B: Monitor in Control Center

```
http://localhost:9021

```

- View topics: `students`, `student-enrollments`, `student-profiles`
- Check message flow in real-time

#### Option C: Check Application Logs

```bash
docker logs -f data-service-student-enrollment
```

### 4. **Stop Everything**

```bash
make stop-stack
```

## What Gets Generated

### Student Records

```json
{
  "student_id": "STU20261001",
  "state_student_id": "ST456789",
  "first_name": "Alice",
  "last_name": "Johnson",
  "date_of_birth": "2012-05-15",
  "grade_level": "7",
  "updated_at": 1740000000000
}
```

### Enrollment Records

```json
{
  "enrollment_id": "ENR2026100001",
  "student_id": "STU20261001",
  "school_id": "Lincoln High School",
  "district_id": "District 01",
  "school_year": "2025-2026",
  "entry_date": "2025-09-01",
  "exit_date": null,
  "enrollment_status": "ACTIVE",
  "updated_at": 1740000000000
}
```

### Output Topics

**students** → KtableProcessor → **STUDENTS-MV** State Store
**student-enrollments** → KtableProcessor → **ENROLLMENTS-MV** State Store
Join Result → **student-profiles** Topic + Cassandra **student_profiles** Table

## Data Characteristics

- **Student IDs:** STU20261001, STU20261002, ... (sequential, by year)
- **Enrollment IDs:** ENR2026100001, ENR2026100002, ... (sequential, by year)
- **Names:** Realistic first and last names (24 options each)
- **Districts:** 5 sample districts with multiple schools per district
- **Grades:** K through 12
- **Status:** 80% ACTIVE, 20% INACTIVE/ON_LEAVE
- **Dates:** Random entry dates within last 2 years, realistic exit dates

## Troubleshooting

### Connection Issues

```
✗ Cannot connect to Kafka at localhost:9092
  Make sure to run: make run-stack
```

**Solution:** Start the full stack first: `make run-stack`

### Schema Registry Warnings

```
⚠ Schema Registry not reachable at http://localhost:8081
  This is optional for demo purposes
```

**Solution:** This is just a warning. The generator will use JSON serialization as fallback.

### Python Package Issues

```
ModuleNotFoundError: No module named 'confluent_kafka'
```

**Solution:** Install dependencies: `pip install -r requirements.txt`

### Permission Denied (Linux/Mac)

```bash
chmod +x data_generator.py
./data_generator.py
```

## Advanced Usage

### Run with Custom Kafka Server

```bash
python data_generator.py -b kafka.example.com:9092 -r 100
```

### Generate Large Dataset

```bash
# 1000 students with minimal interval
python data_generator.py -r 1000 -i 0.1
```

### Continuous Generation Script

```bash
# Generate 50 pairs every minute
while true; do
  python data_generator.py -r 50 -i 0.5
  echo "Waiting 60 seconds..."
  sleep 60
done
```

## Integration with Make

You can add commands to the main Makefile to make it easier:

```makefile
# In Makefile
generate-data:
	cd data-generator && pip install -r requirements.txt && python data_generator.py --records 50 --interval 1

generate-demo:
	cd data-generator && pip install -r requirements.txt && python data_generator.py --records 100 --interval 0.5
```

Then run: `make generate-data`

## Architecture

```
Data Generator (Python)
    ↓
Kafka Topics (students, student-enrollments)
    ↓
Kafka Streams Topology
    ├─→ KtableProcessor (creates KTables)
    └─→ JoinProcessor (joins on student_id)
    ↓
Kafka Topics (student-profiles)
    ↓
Cassandra (flipfoundry.student_profiles)
    ↓
Control Center UI (http://localhost:9021)
```

## Source Code

- [data_generator.py](data_generator.py)
- [../src/main/java/com/flipfoundry/platform/data/service/DataServiceApplication.java](../src/main/java/com/flipfoundry/platform/data/service/DataServiceApplication.java)
- [../src/main/java/com/flipfoundry/platform/data/service/kafka/processor/KtableProcessor.java](../src/main/java/com/flipfoundry/platform/data/service/kafka/processor/KtableProcessor.java)
- [../src/main/java/com/flipfoundry/platform/data/service/kafka/processor/JoinProcessor.java](../src/main/java/com/flipfoundry/platform/data/service/kafka/processor/JoinProcessor.java)
