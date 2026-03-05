# Confluent Platform (Kafka, Schema Registry, Control Center)

## Running Confluent Locally

From the project root, run:
```shell
make run-confluent    # Start
make stop-confluent   # Stop
make logs-confluent   # View logs
```

Alternatively, from the confluent folder:
```shell
docker-compose up -d   # Start
docker-compose down    # Stop
```

## Control Center

Access the Confluent Control Center UI at:
```
http://localhost:9021
```

## Kafka Topics for Student Enrollment Service

The application expects the following topics to exist. Create them via Control Center or using the kafka CLI:

```
students                 # Raw student data from source systems
student-enrollments      # Raw student enrollment data from source systems
student-profiles         # Joined/enriched student profile data (output)
students-dlq             # Dead letter queue for student processing failures
enrollments-dlq          # Dead letter queue for enrollment processing failures
```

### Topic Configuration (Defaults)
- Partitions: 1 (for local development, increase for production)
- Replication Factor: 1
- Cleanup Policy: delete (with compaction for state topics)

## Schema Registry

Access the Schema Registry at:
```
http://localhost:8081
```

The following Avro schemas should be registered:

1. **Student Key & Value Schemas**
   - Key: com.flipfoundry.platform.data.service.avro.Student (string key)
   - Value: com.flipfoundry.platform.data.service.avro.Student

2. **StudentEnrollment Key & Value Schemas**
   - Key: com.flipfoundry.platform.data.service.avro.StudentEnrollment (string key)
   - Value: com.flipfoundry.platform.data.service.avro.StudentEnrollment

3. **StudentProfile Key & Value Schemas**
   - Key: com.flipfoundry.platform.data.service.avro.StudentProfile (string key)
   - Value: com.flipfoundry.platform.data.service.avro.StudentProfile

## Full Stack Startup

To start the entire stack (Cassandra + Confluent + Application):

```shell
make run-stack    # Start all services
make stop-stack   # Stop all services
make status       # Check status
```

## Service Endpoints

- Kafka Broker: `localhost:9092` (external), `broker:29092` (internal)
- Schema Registry: `http://localhost:8081`
- Control Center: `http://localhost:9021`