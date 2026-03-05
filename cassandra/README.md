# Cassandra Setup for Student Enrollment Data Service

## Quick Start

From the project root:
```shell
make run-cassandra    # Start Cassandra
make stop-cassandra   # Stop Cassandra
make logs-cassandra   # View logs

# Or start the entire stack
make run-stack        # Start Cassandra + Confluent + Application
make stop-stack       # Stop all
```

## Manual Commands

From the cassandra folder or project root:
```shell
docker-compose up -d    # Start
docker-compose down     # Stop
docker-compose logs -f  # View logs
```

## Automatic Initialization

The docker-compose configuration automatically:
1. Starts Cassandra on port 9042
2. Waits for startup to complete
3. Creates the `flipfoundry` keyspace
4. Creates required tables via init-db service

## Connect to Cassandra

```shell
docker exec -it cass cqlsh -u cassandra -p cassandra
```

Common CQL commands:
```cql
USE flipfoundry;
DESCRIBE TABLES;
SELECT * FROM student_profiles;
```

## Reset Data

```shell
make stop-cassandra
rm -rf ./data/cass/*
make run-cassandra
```

## Documentation

To use cqlsh you will need to install python https://www.python.org/downloads/release/python-397/

Once installed download `cqlsh` https://downloads.datastax.com/enterprise/cqlsh-astra-20201104-bin.tar.gz

Python will need to be on your path. Run `python --version`.

Unpack the `cqlsh-astra` and navigate to `.\cqlsh-astra\bin`. and run the following command. (make sure you've started cassandard using `docker-compose up -d`)

```
If running powershell you will require a code page.
1   chcp 65001 
2   python cqlsh.py 127.0.0.1 9042 -u cassandra -p cassandra
```


Alternatively you can use the cqlsh in cassandra container by running the following:
```
docker exec -it cass cqlsh -u cassandra -p cassandra

```

## Useful Commands

```
1   Create a keyspace.
create keyspace flipfoundry with replication={'class':'SimpleStrategy', 'replication_factor':1};

2   Use the create keyspace.
use flipfoundry;

3   Create a table.
CREATE TABLE tutorial(id timeuuid PRIMARY KEY,title text,description text,published boolean);

4   Create a custom index on the table.
CREATE CUSTOM INDEX idx_title ON flipfoundry.tutorial (title) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS','analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer','case_sensitive': 'false'};

5   Other useful commands.
DESCRIBE TABLE tutorial;
DESCRIBE tables;
select data_center from system.local;
SELECT * FROM flipfoundry.tutorial;
```