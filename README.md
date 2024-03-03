# kafka-smt

Single message transformer for Zip Mysql database. We require routing messages based on organization guid.


## Prerequisites
Please install both `maven` and `java >= 11`. 

We run Debezium connector using AWS MSK Connect. As of 03/02/2024, the runtime Java version used by MSK Connect is 11, 
so this project is compiled with `-release=11`. Using a release version higher than 11 will lead to below runtime error.

```
[Worker-07f756707cf5fbae8] Caused by: java.lang.UnsupportedClassVersionError: 
com/ziphq/kafka/connect/smt/PartitionRouting has been compiled by a more recent version of the Java 
Runtime (class file version 61.0), this version of the Java Runtime only recognizes class file versions up to 55.0
```

## How to develop

### Build & test
```
# build the jar file
mvn package

# build the jar file without running unit tests
mvn package -DskipTests

# run unit tests
mvn test

# Apply linter 
mvn spotless:apply
```

### Prepare the package
In order to do end-to-end test, the jar file produced by this repo should be packaged together with Debezium and a 
few other dependencies. Also, all jar files should be placed inside the Kafka Connect plug-in folder.

Below is sample function to prepare these jar files. Change it accordingly for your env.
```bash
build_debezium() {
    local smt_jar="~/code/kafka-smt/target/partition-routing-1.0.jar"
    local kafka_connect_plugin_dir="/tmp/kafka-connect/plugins"
    
    pushd . 

    rm -rf /tmp/tmpwork && mkdir -p /tmp/tmpwork && cd /tmp/tmpwork

    # download Debezium 
    wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.5.1.Final/debezium-connector-mysql-2.5.1.Final-plugin.tar.gz
    tar xvzf debezium-connector-mysql-2.5.1.Final-plugin.tar.gz

    # download kafka aws secret manager from https://www.confluent.io/hub/jcustenborder/kafka-config-provider-aws
    wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/jcustenborder/kafka-config-provider-aws/versions/0.1.2/jcustenborder-kafka-config-provider-aws-0.1.2.zip
    unzip jcustenborder-kafka-config-provider-aws-0.1.2.zip
    mv jcustenborder-kafka-config-provider-aws-0.1.2/lib/* debezium-connector-mysql

    # Download Guava. See https://github.com/jcustenborder/kafka-config-provider-aws/issues/2
    wget https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar
    mv guava-31.1-jre.jar debezium-connector-mysql

    # Add kafka-smt
    mv $smt_jar debezium-connector-mysql 

    # move to plugin folder
    rm -rf ${kafka_connect_plugin_dir}/debezium
    mkdir -p ${kafka_connect_plugin_dir}/debezium 
    mv debezium-connector-mysql/* ${kafka_connect_plugin_dir}/debezium/

    popd
}
```

### Release a new version

We need to upload the plugin to s3 to release a new version.
```bash
export kafka_connect_plugin_dir="/tmp/kafka-connect/plugins"
cd ${kafka_connect_plugin_dir}/debezium
zip -9 ../debezium-connector-mysql-2.5.1-aws-config-provider-0.1.2.zip *
cd ..
aws s3 cp debezium-connector-mysql-2.5.1-aws-config-provider-0.1.2.zip s3://zip-kafka-connect-plugins/ --profile=admin
```



### End-to-end test

1. Build the plugin and put it under the plugin folder
2. Create the connector.
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
    '
    ```

3. If connector config does not change, then just restart kafka connect.  
    Otherwise, we also need to delete teh connector and recreate it. Remove the connector if exits
    ```bash 
    curl -X DELETE http://localhost:8083/connectors/binlog-1
    ```


## TODO:
1. Add one more unit test for DDL event.
2. Set up workflow to run unit test and linter.
