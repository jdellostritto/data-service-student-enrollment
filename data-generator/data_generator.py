#!/usr/bin/env python3
"""
DataGenerator for Student Enrollment Service

Generates realistic student and enrollment data and publishes to Kafka topics
with proper Avro serialization. Designed to work with the data-service-student-enrollment
Kafka Streams application.

Usage:
    python data_generator.py
    python data_generator.py --records 100 --interval 1
    python data_generator.py --students-only
"""

import json
import random
import time
import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, Tuple, List
from io import BytesIO

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Avro schemas
STUDENT_SCHEMA = {
    "type": "record",
    "name": "Student",
    "namespace": "com.flipfoundry.platform.data.service.avro",
    "fields": [
        {"name": "student_id", "type": "string"},
        {"name": "state_student_id", "type": ["null", "string"], "default": None},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "date_of_birth", "type": ["null", "string"], "default": None},
        {"name": "grade_level", "type": ["null", "string"], "default": None},
        {"name": "updated_at", "type": ["null", "long"], "default": None}
    ]
}

STUDENT_ENROLLMENT_SCHEMA = {
    "type": "record",
    "name": "StudentEnrollment",
    "namespace": "com.flipfoundry.platform.data.service.avro",
    "fields": [
        {"name": "enrollment_id", "type": "string"},
        {"name": "student_id", "type": "string"},
        {"name": "school_id", "type": "string"},
        {"name": "district_id", "type": "string"},
        {"name": "school_year", "type": "string"},
        {"name": "entry_date", "type": ["null", "string"], "default": None},
        {"name": "exit_date", "type": ["null", "string"], "default": None},
        {"name": "enrollment_status", "type": ["null", "string"], "default": None},
        {"name": "updated_at", "type": ["null", "long"], "default": None}
    ]
}

STUDENT_KEY_SCHEMA = {
    "type": "record",
    "name": "StudentKey",
    "namespace": "com.flipfoundry.platform.data.service.avro",
    "fields": [
        {"name": "student_id", "type": "string"}
    ]
}

STUDENT_ENROLLMENT_KEY_SCHEMA = {
    "type": "record",
    "name": "StudentEnrollmentKey",
    "namespace": "com.flipfoundry.platform.data.service.avro",
    "fields": [
        {"name": "enrollment_id", "type": "string"}
    ]
}


class KafkaProducerManager:
    """Manages Kafka producer and schema registry"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092", 
                 schema_registry_url: str = "http://localhost:8081",
                 mock: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.mock = mock
        self._serializers = {}
        
        if not mock:
            try:
                self.schema_registry = SchemaRegistryClient({'url': schema_registry_url})
                logger.info(f"Connected to Schema Registry at {schema_registry_url}")
            except Exception as e:
                logger.warning(f"Failed to connect to Schema Registry: {e}")
                self.schema_registry = None
            
            self.producer_config = {
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'data-generator'
            }
            
            try:
                self.producer = Producer(self.producer_config)
                logger.info(f"Connected to Kafka at {bootstrap_servers}")
            except Exception as e:
                logger.error(f"Failed to create Kafka producer: {e}")
                raise
            
            # Build Avro serializers for keys and values
            if self.schema_registry:
                self._serializers = {
                    'students-key':   self._make_serializer(STUDENT_KEY_SCHEMA),
                    'students-value': self._make_serializer(STUDENT_SCHEMA),
                    'student-enrollments-key':   self._make_serializer(STUDENT_ENROLLMENT_KEY_SCHEMA),
                    'student-enrollments-value': self._make_serializer(STUDENT_ENROLLMENT_SCHEMA),
                }
        else:
            self.schema_registry = None
            self.producer = None

    def _make_serializer(self, schema: Dict) -> AvroSerializer:
        return AvroSerializer(
            self.schema_registry,
            json.dumps(schema),
            lambda obj, ctx: obj
        )

    def _serialize_key(self, topic: str, key_dict: Dict) -> bytes:
        serializer = self._serializers.get(f'{topic}-key')
        if serializer:
            return serializer(key_dict, SerializationContext(topic, MessageField.KEY))
        return json.dumps(key_dict).encode('utf-8')

    def _serialize_value(self, topic: str, value: Dict) -> bytes:
        serializer = self._serializers.get(f'{topic}-value')
        if serializer:
            return serializer(value, SerializationContext(topic, MessageField.VALUE))
        return json.dumps(value).encode('utf-8')
    
    def produce(self, topic: str, key: str, value: Dict) -> bool:
        """Publish message to Kafka topic"""
        if self.mock or not self.producer:
            logger.debug(f"[MOCK] Topic: {topic}, Key: {key}, Value: {json.dumps(value)}")
            return True
        
        try:
            if topic == 'students':
                key_dict = {"student_id": key}
            else:
                key_dict = {"enrollment_id": key}

            serialized_key = self._serialize_key(topic, key_dict)
            serialized_value = self._serialize_value(topic, value)

            self.producer.produce(
                topic=topic,
                key=serialized_key,
                value=serialized_value,
                on_delivery=self._delivery_report
            )
            self.producer.poll(0)
            return True
        except Exception as e:
            logger.error(f"Failed to produce message to {topic}: {e}")
            return False
    
    def _delivery_report(self, err, msg):
        """Kafka delivery report callback"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def flush(self):
        """Flush pending messages"""
        if self.producer:
            self.producer.flush(30)
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.flush(30)


class DataGenerator:
    """Generates realistic student enrollment data"""
    
    # Sample data
    FIRST_NAMES = [
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
        "Iris", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter",
        "Quinn", "Rachel", "Samuel", "Tina", "Uma", "Victor", "Wendy", "Xavier"
    ]
    
    LAST_NAMES = [
        "Anderson", "Brown", "Chen", "Davis", "Evans", "Fisher", "Garcia", "Harris",
        "Ito", "Johnson", "Kumar", "Lewis", "Martinez", "Nelson", "O'Brien", "Patel",
        "Quinn", "Rodriguez", "Smith", "Thompson", "Ueda", "Vasquez", "Williams", "Young"
    ]
    
    DISTRICTS = [
        "District 01", "District 02", "District 03", "District 04", "District 05"
    ]
    
    SCHOOLS = {
        "District 01": ["Lincoln High School", "Jefferson Middle School", "Adams Elementary"],
        "District 02": ["Kennedy High School", "Roosevelt Middle School", "Monroe Elementary"],
        "District 03": ["Washington High School", "Madison Middle School", "Jackson Elementary"],
        "District 04": ["Harrison High School", "Grant Middle School", "Polk Elementary"],
        "District 05": ["Taylor High School", "Tyler Middle School", "Taft Elementary"],
    }
    
    GRADES = ["K", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
    
    ENROLLMENT_STATUSES = ["ACTIVE", "INACTIVE", "ON_LEAVE"]
    
    def __init__(self):
        self.student_counter = 0
        self.enrollment_counter = 0
        self.current_year = datetime.now().year
    
    def generate_student_id(self) -> str:
        """Generate unique student ID"""
        self.student_counter += 1
        return f"STU{self.current_year}{1000 + self.student_counter:05d}"
    
    def generate_enrollment_id(self) -> str:
        """Generate unique enrollment ID"""
        self.enrollment_counter += 1
        return f"ENR{self.current_year}{100000 + self.enrollment_counter:06d}"
    
    def generate_date_of_birth(self) -> str:
        """Generate realistic date of birth for school-age children (K-12: ~5-18 years old)"""
        age = random.randint(5, 18)
        dob = datetime.now() - timedelta(days=age * 365 + random.randint(0, 365))
        return dob.strftime("%Y-%m-%d")
    
    def generate_student(self) -> Tuple[str, Dict]:
        """Generate a student record"""
        student_id = self.generate_student_id()
        first_name = random.choice(self.FIRST_NAMES)
        last_name = random.choice(self.LAST_NAMES)
        grade_level = random.choice(self.GRADES)
        
        student = {
            "student_id": student_id,
            "state_student_id": f"ST{random.randint(100000, 999999)}",
            "first_name": first_name,
            "last_name": last_name,
            "date_of_birth": self.generate_date_of_birth(),
            "grade_level": grade_level,
            "updated_at": int(time.time() * 1000)
        }
        
        return student_id, student
    
    def generate_enrollment(self, student_id: str) -> Tuple[str, Dict]:
        """Generate enrollment record for a student"""
        enrollment_id = self.generate_enrollment_id()
        district = random.choice(self.DISTRICTS)
        schools = self.SCHOOLS[district]
        school = random.choice(schools)
        
        # Random entry date within the last 2 years
        entry_days_ago = random.randint(30, 730)
        entry_date = (datetime.now() - timedelta(days=entry_days_ago)).strftime("%Y-%m-%d")
        
        # 80% of students are active (no exit date)
        exit_date = None
        status = "ACTIVE"
        if random.random() > 0.8:
            exit_days_ago = random.randint(0, entry_days_ago - 1)
            exit_date = (datetime.now() - timedelta(days=exit_days_ago)).strftime("%Y-%m-%d")
            status = random.choice(["INACTIVE", "ON_LEAVE"])
        
        enrollment = {
            "enrollment_id": enrollment_id,
            "student_id": student_id,
            "school_id": school,
            "district_id": district,
            "school_year": f"{self.current_year - 1}-{self.current_year}",
            "entry_date": entry_date,
            "exit_date": exit_date,
            "enrollment_status": status,
            "updated_at": int(time.time() * 1000)
        }
        
        return enrollment_id, enrollment


def check_kafka_connectivity(bootstrap_servers: str) -> bool:
    """Check if Kafka is running"""
    try:
        import socket
        host, port = bootstrap_servers.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        if result == 0:
            logger.info(f"✓ Kafka is reachable at {bootstrap_servers}")
            return True
        else:
            logger.warning(f"⚠ Cannot connect to Kafka at {bootstrap_servers} (port unreachable)")
            logger.warning("  Note: Continue if Kafka is running behind Docker/internal network")
            return True  # Continue anyway - actual connection attempt will validate
    except Exception as e:
        logger.warning(f"⚠ Precheck failed for {bootstrap_servers}: {e}")
        logger.warning("  Note: Continuing - Kafka may be running on internal network")
        return True  # Continue anyway - actual connection attempt will validate


def check_schema_registry(schema_registry_url: str) -> bool:
    """Check if Schema Registry is running"""
    try:
        import urllib.request
        response = urllib.request.urlopen(f"{schema_registry_url}/subjects", timeout=2)
        logger.info(f"✓ Schema Registry is reachable at {schema_registry_url}")
        return True
    except Exception:
        logger.warning(f"⚠ Schema Registry not reachable at {schema_registry_url}")
        logger.warning("  This is optional for demo purposes")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Generate and publish student enrollment data to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 50 students and 50 enrollments, publish immediately
  python data_generator.py --records 50
  
  # Generate 100 students only (no enrollments)
  python data_generator.py --records 100 --students-only
  
  # Generate 25 pairs with 2 second interval between pairs
  python data_generator.py --records 25 --interval 2
  
  # Mock run (no Kafka connection)
  python data_generator.py --mock --records 10
        """
    )
    
    parser.add_argument('-r', '--records', type=int, default=25,
                        help='Number of (student, enrollment) pairs to generate (default: 25)')
    parser.add_argument('-i', '--interval', type=float, default=0.5,
                        help='Interval in seconds between publishing records (default: 0.5)')
    parser.add_argument('-s', '--students-only', action='store_true',
                        help='Generate students only, without enrollments')
    parser.add_argument('-b', '--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('-sr', '--schema-registry', default='http://localhost:8081',
                        help='Schema Registry URL (default: http://localhost:8081)')
    parser.add_argument('-m', '--mock', action='store_true',
                        help='Run in mock mode without connecting to Kafka')
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("Student Enrollment Data Generator")
    logger.info("=" * 70)
    
    # Check connectivity (unless in mock mode)
    if not args.mock:
        check_kafka_connectivity(args.bootstrap_servers)
        check_schema_registry(args.schema_registry)
    
    # Initialize components
    generator = DataGenerator()
    producer = KafkaProducerManager(
        bootstrap_servers=args.bootstrap_servers,
        schema_registry_url=args.schema_registry,
        mock=args.mock
    )
    
    # Register schemas
    if not args.mock:
        logger.info("Avro serializers initialized (schemas auto-registered with Schema Registry)")
    
    logger.info(f"\nGenerating data:")
    logger.info(f"  Records: {args.records}")
    if not args.students_only:
        logger.info(f"  Interval: {args.interval}s between records")
    logger.info(f"  Bootstrap Servers: {args.bootstrap_servers}")
    logger.info("")
    
    students_published = 0
    enrollments_published = 0
    
    try:
        for i in range(args.records):
            # Generate and publish student
            student_id, student = generator.generate_student()
            if producer.produce("students", student_id, student):
                students_published += 1
                logger.info(f"[{i+1}/{args.records}] Published student: {student_id} "
                           f"({student['first_name']} {student['last_name']})")
            
            # Generate and publish enrollment (unless --students-only)
            if not args.students_only:
                enrollment_id, enrollment = generator.generate_enrollment(student_id)
                if producer.produce("student-enrollments", enrollment_id, enrollment):
                    enrollments_published += 1
                    logger.info(f"[{i+1}/{args.records}] Published enrollment: {enrollment_id} "
                               f"({enrollment['school_id']})")
            
            # Sleep between publications
            if i < args.records - 1:
                time.sleep(args.interval)
        
        # Flush remaining messages
        producer.flush()
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("Generation Complete!")
        logger.info("=" * 70)
        logger.info(f"Students published: {students_published}")
        logger.info(f"Enrollments published: {enrollments_published}")
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Check Cassandra: docker exec -it cass cqlsh -u cassandra -p cassandra")
        logger.info("   SELECT * FROM student_enrollment.student_profiles;")
        logger.info("")
        logger.info("2. View in Control Center: http://localhost:9021")
        logger.info("")
        logger.info("3. Query state stores (when running): GET /actuator/health")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Error during generation: {e}")
        sys.exit(1)
    finally:
        producer.close()


if __name__ == '__main__':
    main()
