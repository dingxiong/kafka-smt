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
or 
```
mvn package -DskipTests
```

## Test
```
mvn test
```

## Prepare the package
```bash
build_debezium() {
    pushd . 

    rm -rf /tmp/tmpwork && mkdir -p /tmp/tmpwork
    cd /tmp/tmpwork

    wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.5.1.Final/debezium-connector-mysql-2.5.1.Final-plugin.tar.gz
    tar xvzf debezium-connector-mysql-2.5.1.Final-plugin.tar.gz

    # Then, download kafka aws secret manager from https://www.confluent.io/hub/jcustenborder/kafka-config-provider-aws
    wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/jcustenborder/kafka-config-provider-aws/versions/0.1.2/jcustenborder-kafka-config-provider-aws-0.1.2.zip
    unzip jcustenborder-kafka-config-provider-aws-0.1.2.zip
    mv jcustenborder-kafka-config-provider-aws-0.1.2/lib/* debezium-connector-mysql

    # Download Guava. See https://github.com/jcustenborder/kafka-config-provider-aws/issues/2
    wget https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar
    mv guava-31.1-jre.jar debezium-connector-mysql

    # Add kafka-smt
    mv ~/code/kafka-smt/target/partition-routing-1.0.jar debezium-connector-mysql 

    # move to plugin folder
    rm -rf /tmp/kafka-connect/plugins/debezium
    mkdir -p /tmp/kafka-connect/plugins/debezium 
    mv debezium-connector-mysql/* /tmp/kafka-connect/plugins/debezium/

    popd
}
```

To upload the plugin to s3 
```
cd /tmp/kafka-connect/plugins/debezium/
zip -9 ../debezium-connector-mysql-2.5.1-aws-config-provider-0.1.2.zip *
cd ..
aws s3 cp debezium-connector-mysql-2.5.1-aws-config-provider-0.1.2.zip s3://zip-kafka-connect-plugins/ --profile=admin
```


If connector config is not changed, then just restart kafka connect.
Otherwise, we also need to delete teh connector and recreate it.


Remove the connector if exits
```bash 
curl -X DELETE http://localhost:8083/connectors/binlog-1
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
                "transforms.PartitionRouting.partition.topic.num": 6,
                "transforms.PartitionRouting.partition.hash.function": "murmur"
    }
}
'
```

## TODO:
1. Add one more unit test for DDL event.
2. Set up Java linter.
3. Set up workflow to run unit test and linter.