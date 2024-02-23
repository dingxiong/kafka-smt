# kafka-smt

Single message transformer

## Initial setup

```bash
cd /tmp/
mvn archetype:generate -DgroupId=com.ziphq.kafka.connect.smt -DartifactId=partition-routing -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 -DinteractiveMode=false
mv partition-routing/* kafka-smt
```

## Build & package
```
mvn package
```

## Test
```
mvn test
```

```bash
curl -X POST -H "Content-Type: application/json" localhost:8083/connectors -d '
{
    "name": "binlog-1", 
    "config": {
				"errors.log.include.messages": true,
                "errors.log.enable": true,
                "connector.class": "io.debezium.connector.mysql.MySqlConnector", 
				"tasks.max": 1,
                "database.connectionTimeZone": "America/Los_Angeles",
				"value.converter": "org.apache.kafka.connect.json.JsonConverter",
				"value.converter.schemas.enable": true,
				"key.converter": "org.apache.kafka.connect.json.JsonConverter",
				"key.converter.schemas.enable": true,
				"tombstones.on.delete": false,
				"database.include.list": "admincoin",
				"include.schema.changes": true,
				"topic.prefix": "database-1",
				"schema.history.internal.kafka.topic": "database-1-schema-change",
				"database.hostname": "localhost",
				"database.user": "root",
				"database.password": "password",
				"database.server.id": "30",
				"database.port": 3306,
                "schema.history.internal.kafka.bootstrap.servers": "localhost:9092", 
				"max.batch.size": 100,
				"snapshot.mode": "schema_only",
				"snapshot.locking.mode": "none",
				"topic.creation.default.partitions": 6,
				"topic.creation.default.replication.factor": 1,
				"topic.creation.default.cleanup.policy": "delete",
				"topic.creation.default.compression.type": "lz4",
				"transforms": "Reroute,PartitionRouting",
				"transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
				"transforms.Reroute.topic.regex": "(.*)",
				"transforms.Reroute.topic.replacement": "database-1.admincoin.mutations",
			    "transforms.PartitionRouting.type": "com.ziphq.kafka.connect.smt.PartitionRouting",
                "transforms.PartitionRouting.partition.payload.fields": "change.name",
                "transforms.PartitionRouting.partition.topic.num": 2
    }
}
'
```