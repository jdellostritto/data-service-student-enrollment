# Student Enrollment Data Service

A modern Kafka Streams application for processing student enrollment data with Cassandra persistence and Docker Compose orchestration.

**Quick Start:** See [QUICKSTART.md](QUICKSTART.md) for 5-minute setup.

---

## Purpose

The Student Enrollment Data Service ingests student and enrollment data from source systems, processes them through Kafka Streams topology, and materializes the results in Cassandra for efficient querying.

**Key Functions:**
- **Validate** incoming student and enrollment records
- **Enrich** by joining student data with enrollment records  
- **Deduplicate** out-of-order events (keeps latest by timestamp)
- **Persist** results in Cassandra for low-latency access
- **Materialize** Kafka Streams state stores for interactive queries

**Use Cases:**
- Real-time student profile materialization
- Multi-source data consolidation
- Event-driven enrollment updates
- Ad-hoc student record queries via state stores

---

## Architecture

### System Overview

![Student Profile Join Topology](student_profile_join.png)

### Stream Processing Topology

#### Stage 1: Input Validation (KtableProcessor)

**Node 1a - Deserialization Check:**
- Consume raw bytes from source topics
- Check for null values (tombstones)
- Branch into valid and invalid streams
- **Valid:** Continue to Node 1b
- **Invalid:** Send to DLQ (dead letter queue)

**Node 1b - Avro Deserialization:**
- Deserialize valid records from raw bytes to Avro objects
- Student → `Student` Avro class
- StudentEnrollment → `StudentEnrollment` Avro class

**Node 1c - KTable Materialization (Reduce Logic):**
- Create `STUDENTS-MV` state store (Latest student by timestamp)
- Create `ENROLLMENTS-MV` state store (Latest enrollment by timestamp)
- Keep only the newest record for each key (deduplication)

#### Stage 2: Join & Enrichment (JoinProcessor)

**Node 2 - Foreign Key Join:**
- Join `ENROLLMENTS-MV` KTable with `STUDENTS-MV` KTable
- Link: `student_id` in enrollment → `student_id` in student
- Produces combined `StudentProfile` records
- Materializes `STUDENT-PROFILES-MV` state store

#### Stage 3: Persistence

**Node 3 - Cassandra Write:**
- Extract fields from `StudentProfile` Avro object
- Map to `StudentProfile` entity model
- Persist to `student_profiles` Cassandra table
- Null handling with defaults via custom BeanUtils

**Node 4 - Kafka Topic (Durable Log):**
- Output topic: `student-profiles`
- Provides event sourcing capability
- Enables replay and recovery

### Data Models

**Student**
```
student_id* (PK)
├─ state_student_id
├─ first_name
├─ last_name
├─ date_of_birth
├─ grade_level
└─ updated_at (timestamp for deduplication)
```

**StudentEnrollment**
```
enrollment_id* (PK)
├─ student_id (FK → Student)
├─ school_id
├─ district_id  
├─ school_year
├─ entry_date
├─ exit_date
├─ enrollment_status (ACTIVE | INACTIVE | ON_LEAVE)
└─ updated_at (timestamp for deduplication)
```

**StudentProfile** (Join Result - Denormalized)
```
student_id* (PK)
├─ (all Student fields)
├─ enrollment_id
├─ (all StudentEnrollment fields)
└─ (both updated_at timestamps)
```

### Key Design Patterns

**KTable Reduction (Deduplication):**
```
Ingestion Order: Event1 (ts=100) → Event2 (ts=50) → Event3 (ts=75)
Reduction Logic: Keep only latest by timestamp
Output State:    Event1 (ts=100) ← Newest
```

**Foreign Key Join:**
- Joins across non-identical keys (enrollment.student_id → student.student_id)
- Requires both sides to be KTables (co-partitioned)
- Streams automatically handles distributed state synchronization

**Compacted Topics:**
- `students` and `student-enrollments` use log compaction
- Only latest value per key retained on disk
- Enables KTable reconstruction from topic replay
- Configurable: `cleanup.policy=compact`

---

## Operations

### Service Startup

```bash
# Full stack (Cassandra + Kafka + Application)
make run-stack

# Individual services
make run-cassandra      # Cassandra only
make run-confluent      # Kafka/Confluent only  
make run-app            # Application only (requires Cassandra + Kafka)

# Verification
make status             # Check all services
docker ps               # List running containers
```

### Management Commands

#### Data Management
```bash
cd data-generator

# Generate test data
python data_generator.py --records 50              # 50 pairs
python data_generator.py --records 100 --students-only  # Students only
python data_generator.py --mock --records 10       # Mock mode (no Kafka)

# Install dependencies
pip install -r requirements.txt

# Full options
python data_generator.py --help
```

#### Build & Test
```bash
make build              # Gradle build
make test               # Run unit tests
make clean              # Clean artifacts
make image              # Build Docker image (Jib)
make bootrun            # Run locally (Spring Boot)
```

#### Monitoring
```bash
# Logs
make logs               # All service logs
make logs-cassandra     # Cassandra only
make logs-confluent     # Kafka/Confluent only
docker logs -f <container_name>  # Follow specific service

# Status
docker ps               # List containers
docker stats            # Resource usage
```

#### Cleanup
```bash
make stop-stack         # Stop all services
make prune              # Docker cleanup
docker system prune -a  # Full cleanup (careful!)
```

### End-to-End Workflow

**Step 1: Start Stack**
```bash
make run-stack
# Wait for: "Full stack is running!"
```

**Step 2: Generate Data**
```bash
cd data-generator
pip install -r requirements.txt
python data_generator.py --records 50 --interval 1
# Wait for: "Generation Complete!"
```

**Step 3: Monitor Cassandra**
```bash
docker exec -it cass cqlsh -u cassandra -p cassandra

USE student_enrollment;
SELECT count(*) FROM student_profiles;
SELECT * FROM student_profiles LIMIT 10;
```

**Step 4: Monitor Kafka**
```
http://localhost:9021
```
- Cluster → Topics → student-profiles
- View message flow in real-time

**Step 5: Monitor Application**
```bash
docker logs -f data-service-student-enrollment | grep -i "enroll\|student\|join\|reduce"
```

**Step 6: Cleanup**
```bash
make stop-stack
```

### Cassandra Access

```bash
# Connect
docker exec -it cass cqlsh -u cassandra -p cassandra

# Common queries
USE student_enrollment;
DESCRIBE TABLES;
DESCRIBE TABLE students;
DESCRIBE TABLE student_enrollments;
DESCRIBE TABLE student_profiles;

SELECT * FROM students LIMIT 10;
SELECT * FROM student_enrollments LIMIT 10;
SELECT * FROM student_profiles LIMIT 10;

SELECT count(*) FROM student_profiles;

# Exit
EXIT;
```

### Kafka Administration

**Control Center UI:**
```
http://localhost:9021
```

**Via Command Line:**
```bash
# List topics
docker exec -it broker kafka-topics --bootstrap-server broker:9092 --list

# Describe topic
docker exec -it broker kafka-topics --bootstrap-server broker:9092 \
  --describe --topic student-profiles

# View messages
docker exec -it broker kafka-console-consumer --bootstrap-server broker:9092 \
  --topic student-profiles --from-beginning --max-messages 10
```

### Service Endpoints

| Service | URL/Port | Purpose |
|---------|----------|---------|
| Application | http://localhost:8700 | REST API |
| Cassandra | localhost:9042 | Database |
| Kafka Broker | localhost:9092 | Message broker |
| Schema Registry | http://localhost:8081 | Schema management |
| Control Center | http://localhost:9021 | Kafka admin UI |
| Metrics | http://localhost:9001 | Application metrics |

---

## Troubleshooting

### Services Won't Start

**Symptom:** `docker-compose up` fails or containers exit immediately

**Solutions:**
```bash
# Check Docker daemon
docker ps

# Check logs
docker logs <container_name>

# Clean and retry
make stop-stack
docker system prune -a
docker volume prune
make run-stack
```

### Kafka Connection Errors

**Symptom:** 

```
✗ Cannot connect to Kafka at localhost:9092
  Make sure to run: make run-stack
```

**Solutions:**
1. **Verify Kafka is running:**
   ```bash
   docker logs broker | grep "started"
   ```

2. **Wait for Kafka startup (~30 seconds):**
   ```bash
   docker logs broker | tail -20
   ```

3. **Check broker configuration:**
   ```bash
   docker exec -it broker bash
   cat /etc/kafka/server.properties | grep ADVERTISED
   ```

4. **Test connectivity:**
   ```bash
   docker exec -it broker kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

### Cassandra Connection Issues

**Symptom:** 
```
Connection refused: Cassandra at port 9042
```

**Solutions:**
1. **Verify Cassandra is healthy:**
   ```bash
   docker ps | grep cass
   # Should show: "Up (healthy)"
   ```

2. **Wait for startup:**
   ```bash
   docker logs cass | grep "Listening"
   ```

3. **Check keyspace exists:**
   ```bash
   docker exec -it cass cqlsh -u cassandra -p cassandra \
     -c "DESCRIBE KEYSPACES;"
   ```

### Data Not Appearing in Cassandra

**Symptom:** Records don't appear in `student_profiles` table after generation

**Troubleshooting Steps:**

1. **Check data generation:**
   ```bash
   cd data-generator
   python data_generator.py --mock --records 5
   # Should see: "Generation Complete!"
   ```

2. **Verify Kafka topics have messages:**
   ```bash
   docker exec -it broker kafka-console-consumer \
     --bootstrap-server broker:9092 \
     --topic students \
     --from-beginning \
     --max-messages 5
   ```

3. **Check application logs for errors:**
   ```bash
   docker logs data-service-student-enrollment | grep -i error
   ```

4. **Verify tables exist:**
   ```bash
   docker exec -it cass cqlsh -u cassandra -p cassandra \
     -c "USE student_enrollment; DESCRIBE TABLES;"
   ```

5. **Check state stores:**
   - Go to http://localhost:9001/actuator/health
   - Look for state store status in application logs

### Schema Registration Warnings

**Symptom:**
```
⚠ Schema Registry not reachable at http://localhost:8081
  This is optional for demo purposes
```

**This is not an error.** Schema registration is optional. The application can work without it.

**To fix (optional):**
```bash
# Verify Schema Registry is running
docker logs schema-registry

# Test connectivity
curl http://localhost:8081/subjects
```

### Performance Issues

**Slow data processing:**

1. **Check resource usage:**
   ```bash
   docker stats
   ```

2. **Increase concurrency:**
   ```yaml
   # In application-default.yml
   spring.kafka.streams.properties.num.stream.threads: 4
   ```

3. **Tune Kafka settings:**
   ```yaml
   # In application-default.yml
   spring.kafka.streams.properties.cache.max.bytes.buffering: 10485760  # 10MB
   commit.interval.ms: 1000
   ```

4. **Monitor bottlenecks:**
   - Check Cassandra write throughput: `nodetool status`
   - Check Kafka consumer lag: Control Center UI
   - Check JVM memory: `docker stats`

### DLQ Messages (Dead Letter Queue)

**Meaning:** Records failed deserialization and were sent to DLQ topic

**Check DLQ:**
```bash
docker exec -it broker kafka-console-consumer \
  --bootstrap-server broker:9092 \
  --topic students-dlq \
  --from-beginning
```

**Causes:**
- Malformed Avro records
- Schema mismatch
- Null keys or values

**Fix:**
1. Fix the data generator or source system
2. Regenerate data with correct schema
3. Manually delete records from topic if needed

### Out of Memory

**Symptom:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**
```bash
# Increase JVM heap
export JAVA_OPTS="-Xmx2g"
make run-app

# Or modify docker-compose.yml
environment:
  JAVA_OPTS: "-Xmx2g -Xms1g"
```

### Docker Network Issues

**Symptom:** Containers can't reach each other by hostname

**Solutions:**
```bash
# Check network
docker network ls | grep confluent

# Inspect network
docker network inspect flipfoundry

# Ensure all containers on same network
docker inspect broker | grep -A 10 "Networks"
```

### Reset to Clean State

**Warning: This deletes all data**

```bash
# Stop everything
make stop-stack

# Remove containers and volumes
docker-compose -f cassandra/docker-compose.yml down -v
docker-compose -f confluent/docker-compose.yml down -v

# Remove application container
docker rm -f data-service-student-enrollment

# Start fresh
make run-stack
```

---

## Additional Resources

- [QUICKSTART.md](QUICKSTART.md) - 5-minute setup guide
- [data-generator/README.md](data-generator/README.md) - Data generation options and examples
- [CASSMODEL.md](CASSMODEL.md) - Database schema and model details
- [cassandra/README.md](cassandra/README.md) - Cassandra configuration details
- [confluent/README.md](confluent/README.md) - Kafka/Confluent configuration
