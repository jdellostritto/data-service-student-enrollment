# Quick Start - Student Enrollment Data Service

Get the Student Enrollment Data Service running in 5 minutes.

## Prerequisites

- Docker & Docker Compose installed
- Make installed (or Windows with PowerShell)
- Python 3.8+ (for data generation)

## 5-Minute Setup

Start services **sequentially** in separate terminals. Starting them in parallel can cause race conditions.

### 1. Start Cassandra (Terminal 1)

```bash
cd /path/to/data-service-student-enrollment
make run-cassandra
```

Wait for: `Cassandra initialized and ready`

### 2. Start Kafka Stack (Terminal 2)

```bash
cd /path/to/data-service-student-enrollment
make run-confluent
```

Wait for: `Confluent services are running` (usually 30-45 seconds)

**Verify Control Center is running:**
Open your browser and navigate to: `http://localhost:9021`

You should see the Confluent Control Center dashboard. If it's not responding, wait another 10-15 seconds and refresh.

### 3. Start Application (Terminal 3)

```bash
cd /path/to/data-service-student-enrollment
make run-app
```

Wait for: `KafkaStreams started` in the logs

### 4. Generate Sample Data (Terminal 4)

The data generator creates realistic student and enrollment records in Avro format and publishes them to Kafka topics.

```bash
cd data-generator

# Install dependencies (first time only)
pip install -r requirements.txt

# Generate 50 student/enrollment pairs (publishes to Kafka)
python data_generator.py --records 50 --interval 1
```

**What it does:**
- Creates Student records → publishes to `students` topic
- Creates StudentEnrollment records → publishes to `student-enrollments` topic
- Application consumes topics → joins data → stores in Cassandra `student_profiles`

**Common options:**
```bash
--records 25              # Number of student/enrollment pairs (default: 10)
--interval 0.5            # Seconds between publications (default: 1.0)
--students-only           # Only publish students, skip enrollments
--mock                    # Test mode (no Kafka connection required)
```

Watch the output for: `Generation Complete!` and `Successfully published` messages.

### 5. View Results (Terminal 5)

**Option A - Cassandra Query:**
```bash
docker exec -it cass cqlsh -u cassandra -p cassandra

# In cqlsh:
USE student_enrollment;
SELECT count(*) FROM student_profiles;
SELECT * FROM student_profiles LIMIT 5;
```

**Option B - Control Center UI:**
```
http://localhost:9021
```
Navigate to Cluster → Topics → student-profiles

**Option C - Application Logs:**
```bash
docker logs -f data-service-student-enrollment
```

### 6. Cleanup

Stop services in reverse order:

```bash
# Terminal 3 (App): Ctrl+C
# Terminal 2 (Kafka): Ctrl+C
# Terminal 1 (Cassandra): Ctrl+C

# Or stop all at once:
make stop-all
```

---

## Commands Reference

```bash
# Sequential stack management (start in order)
make run-cassandra          # Terminal 1: Start Cassandra
make run-confluent          # Terminal 2: Start Kafka + Schema Registry
make run-app                # Terminal 3: Start application
make generate-data          # Terminal 4: Generate sample data

# Cleanup
make stop-all               # Stop all services at once

# Individual services
make status                 # Show service status
make clean                  # Clean build artifacts

# Data generation
cd data-generator
python data_generator.py --help                              # All options
python data_generator.py --records 100                       # 100 student/enrollment pairs
python data_generator.py --records 50 --interval 0.5         # 50 pairs, publish every 0.5 seconds
python data_generator.py --records 25 --students-only        # Publish only students
python data_generator.py --mock --records 10                 # Test mode (no Kafka required)
python data_generator.py --mock --records 25 --interval 0.1  # Mock - fast testing

# Build and test
make build                  # Build with Gradle
make test                   # Run unit tests
make clean                  # Clean artifacts
```

---

## Troubleshooting Quick Fixes

**Data generator module errors (authlib, httpx, etc):**
```bash
# Reinstall all dependencies
cd data-generator
pip install --upgrade -r requirements.txt
```

**Generator can't connect to Kafka:**
```bash
# Use mock mode to test generator independently
cd data-generator
python data_generator.py --mock --records 10
# Should output student and enrollment records without Kafka
```

**Generator runs but no data appears in Cassandra:**
1. Check Kafka topics exist in Control Center: `http://localhost:9021`
   - Should see `students` and `student-enrollments` topics
2. Verify application is consuming: `docker logs -f data-service-student-enrollment`
3. Check Cassandra has data: `docker exec -it cass cqlsh -u cassandra -p cassandra`
   ```
   USE student_enrollment;
   SELECT count(*) FROM student_profiles;
   ```

**Services won't start:**
```bash
# Check Docker
docker ps

# Clean up
make prune
docker system prune -a
```

**Kafka connection error:**
```bash
# Ensure Kafka is ready (wait ~30 seconds)
docker logs broker | grep "started"
```

**Can't connect to Cassandra:**
```bash
# Check Cassandra is healthy
docker ps | grep cass
# Should show: "Up (healthy)"
```

**Data not appearing in Cassandra:**
1. Check data generator logs for errors
2. Verify Kafka topics exist: `http://localhost:9021`
3. Check application logs: `docker logs -f data-service-student-enrollment`

---

## Next Steps

- See [README.md](README.md) for detailed **Architecture** and **Operations**
- See [DATA_GENERATOR.md](DATA_GENERATOR.md) for advanced data generation options
- See [CASSMODEL.md](CASSMODEL.md) for database schema details
