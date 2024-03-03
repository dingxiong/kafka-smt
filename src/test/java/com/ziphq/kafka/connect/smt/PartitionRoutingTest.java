package com.ziphq.kafka.connect.smt;

import static io.debezium.connector.AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA;
import static io.debezium.data.Envelope.Operation.CREATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.debezium.data.Envelope;
import io.debezium.time.Timestamp;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

public class PartitionRoutingTest {
  public static final Schema OBJECT_SCHEMA =
      SchemaBuilder.struct()
          .name("database-1.admincoin.mutations.Value")
          .field("created_at", Timestamp.builder().optional().build())
          .field("creator_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("data", Schema.BYTES_SCHEMA)
          .field("deleted_at", Timestamp.builder().optional().build())
          .field("deletor_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("id", Schema.INT64_SCHEMA)
          .field("object_type", Schema.OPTIONAL_STRING_SCHEMA)
          .field("organization_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("status", Schema.OPTIONAL_INT32_SCHEMA)
          .field("updated_at", Timestamp.builder().optional().build())
          .field("version", Schema.OPTIONAL_INT32_SCHEMA)
          .field("template_object_type", Schema.OPTIONAL_INT32_SCHEMA)
          .build();
  public static final Schema ASSOCIATION_SCHEMA =
      SchemaBuilder.struct()
          .name("database-1.admincoin.mutations.Value")
          .field("association_type_id", Schema.OPTIONAL_INT32_SCHEMA)
          .field("created_at", Timestamp.builder().optional().build())
          .field("creator_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("data", Schema.OPTIONAL_BYTES_SCHEMA)
          .field("deleted_at", Timestamp.builder().optional().build())
          .field("id", Schema.INT64_SCHEMA)
          .field("metadata1", Schema.OPTIONAL_STRING_SCHEMA)
          .field("metadata2", Schema.OPTIONAL_STRING_SCHEMA)
          .field("sort_order", Schema.OPTIONAL_INT32_SCHEMA)
          .field("source_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("source_id", Schema.OPTIONAL_INT64_SCHEMA)
          .field("status", Schema.OPTIONAL_INT32_SCHEMA)
          .field("target_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("target_id", Schema.OPTIONAL_INT64_SCHEMA)
          .field("updated_at", Timestamp.builder().optional().build())
          .field("source_organization_guid", Schema.OPTIONAL_BYTES_SCHEMA)
          .field("target_organization_guid", Schema.OPTIONAL_BYTES_SCHEMA)
          .build();

  public static final Schema OBJECT_COLUMN_INDEX_SCHEMA =
      SchemaBuilder.struct()
          .name("database-1.admincoin.mutations.Value")
          .field("boolean_value", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("column_name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("datetime_value", Timestamp.builder().optional().build())
          .field("float_value", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("id", Schema.INT64_SCHEMA)
          .field("int_value", Schema.OPTIONAL_INT32_SCHEMA)
          .field("long_value", Schema.OPTIONAL_INT32_SCHEMA)
          .field("object_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("object_type", Schema.OPTIONAL_STRING_SCHEMA)
          .field("organization_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("string_value", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  public static final Schema OBJECT_TYPE_SCHEMA =
      SchemaBuilder.struct()
          .name("database-1.admincoin.mutations.Value")
          .field("id", Schema.INT64_SCHEMA)
          .field("object_type_name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("object_schema", Schema.OPTIONAL_BYTES_SCHEMA)
          .field("version", Schema.OPTIONAL_INT32_SCHEMA)
          .field("status", Schema.OPTIONAL_INT32_SCHEMA)
          .field("created_at", Timestamp.builder().optional().build())
          .field("updated_at", Timestamp.builder().optional().build())
          .field("deleted_at", Timestamp.builder().optional().build())
          .build();

  public static final Schema ACCOUNTING_LEDGER_ACCOUNT_BALANCE_SCHEMA =
      SchemaBuilder.struct()
          .name("database-1.admincoin.mutations.Value")
          .field("object_id", Schema.INT64_SCHEMA)
          .field("object_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("organization_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("object_status", Schema.OPTIONAL_INT32_SCHEMA)
          .field("created_at", Timestamp.builder().optional().build())
          .field("updated_at", Timestamp.builder().optional().build())
          .field("deleted_at", Timestamp.builder().optional().build())
          .field("creator_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("deletor_guid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("account_number", Schema.STRING_SCHEMA)
          .field("amount_su", Schema.INT64_SCHEMA)
          .field("currency", Schema.STRING_SCHEMA)
          .field("template_object_type", Schema.OPTIONAL_INT32_SCHEMA)
          .build();

  //  DDL event

  private static final PartitionRouting<SourceRecord> partitionRoutingTransformation =
      new PartitionRouting<>();

  static {
    Map<String, Object> props = new HashMap<>();
    props.put(PartitionRouting.FIELD_TOPIC_PARTITION_NUM_CONF, 6);
    props.put(PartitionRouting.FIELD_HASH_FUNCTION, "murmur");
    partitionRoutingTransformation.configure(props);
  }

  @Test
  public void bytesToUUID() {
    String uuid =
        PartitionRouting.convertBytesToUUID(
            hexStringToByteArray(("2810200F8F0647F98382DCCBF98D0E40")));
    assertEquals(uuid, "2810200f-8f06-47f9-8382-dccbf98d0e40");
  }

  @Test
  public void routeObjectMessages() {
    final String guid = "8605a5e6-4603-4fd3-803b-0ac3178781bc";
    final SourceRecord eventRecord = buildSourceRecord("object", buildObjectRow(guid), CREATE);

    final Optional<String> orgGuid =
        PartitionRouting.getOrganizationGuid((Struct) eventRecord.value());
    assertEquals(orgGuid, Optional.of(guid));

    SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

    assertEquals(transformed.kafkaPartition(), Integer.valueOf(5));
  }

  @Test
  public void routeAssociationMessages() {
    final String guid = "2810200F8F0647F98382DCCBF98D0E40";
    SourceRecord eventRecord =
        buildSourceRecord("association", buildAssociationRow(guid, guid), CREATE);

    final Optional<String> orgGuid =
        PartitionRouting.getOrganizationGuid((Struct) eventRecord.value());
    assertEquals(orgGuid, Optional.of("2810200f-8f06-47f9-8382-dccbf98d0e40"));

    SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

    assertEquals(transformed.kafkaPartition(), Integer.valueOf(5));

    // source_org_guid != null && target_org_guid == null
    eventRecord = buildSourceRecord("association", buildAssociationRow(guid, null), CREATE);
    transformed = partitionRoutingTransformation.apply(eventRecord);
    assertEquals(transformed.kafkaPartition(), Integer.valueOf(5));

    // source_org_guid != null && target_org_guid == null
    eventRecord = buildSourceRecord("association", buildAssociationRow(null, guid), CREATE);
    transformed = partitionRoutingTransformation.apply(eventRecord);
    assertEquals(transformed.kafkaPartition(), Integer.valueOf(5));

    // source_org_guid == null && target_org_guid == null
    eventRecord = buildSourceRecord("association", buildAssociationRow(null, null), CREATE);
    transformed = partitionRoutingTransformation.apply(eventRecord);
    assertNull(transformed.kafkaPartition());
  }

  @Test
  public void routeObjectColumnIndexMessages() {
    final String guid = "8605a5e6-4603-4fd3-803b-0ac3178781bc";
    final SourceRecord eventRecord =
        buildSourceRecord("object_column_index", buildObjectColumnIndexRow(guid), CREATE);

    final Optional<String> orgGuid =
        PartitionRouting.getOrganizationGuid((Struct) eventRecord.value());
    assertEquals(orgGuid, Optional.of(guid));

    SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

    assertEquals(transformed.kafkaPartition(), Integer.valueOf(5));
  }

  @Test
  public void routeObjectTypeMessages() {
    final SourceRecord eventRecord = buildSourceRecord("object_type", buildObjectTypeRow(), CREATE);

    final Optional<String> orgGuid =
        PartitionRouting.getOrganizationGuid((Struct) eventRecord.value());
    assertEquals(orgGuid, Optional.empty());

    SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

    assertNull(transformed.kafkaPartition());
  }

  @Test
  public void routeBreakoutTableMessages() {
    final String guid = "8605a5e6-4603-4fd3-803b-0ac3178781bc";
    final SourceRecord eventRecord =
        buildSourceRecord(
            "accounting_ledger_account_balance",
            buildAccountingLedgerAccountBalanceRow(guid),
            CREATE);

    final Optional<String> orgGuid =
        PartitionRouting.getOrganizationGuid((Struct) eventRecord.value());
    assertEquals(orgGuid, Optional.of(guid));

    SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

    assertEquals(transformed.kafkaPartition(), Integer.valueOf(5));
  }

  private SourceRecord buildSourceRecord(String table, Struct row, Envelope.Operation operation) {

    // copied from MySqlSourceInfoStructMaker
    Schema sourceSchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector.mysql.Source")
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("snapshot", SNAPSHOT_RECORD_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .field("server_id", Schema.INT64_SCHEMA)
            .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
            .field("file", Schema.STRING_SCHEMA)
            .field("pos", Schema.INT64_SCHEMA)
            .field("row", Schema.INT32_SCHEMA)
            .field("thread", Schema.OPTIONAL_INT64_SCHEMA)
            .field("query", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    Struct source =
        new Struct(sourceSchema)
            .put("connector", "mysql")
            .put("db", "admincoin")
            .put("table", table)
            .put("version", "2.5.1.Final")
            .put("name", "database-1")
            .put("ts_ms", 1709258527000L)
            .put("server_id", 1L)
            .put("file", "binlog.000001")
            .put("pos", 16680L)
            .put("row", 0)
            .put("thread", 93L);

    Schema valueSchema;
    switch (table) {
      case "object":
        valueSchema = OBJECT_SCHEMA;
        break;
      case "association":
        valueSchema = ASSOCIATION_SCHEMA;
        break;
      case "object_column_index":
        valueSchema = OBJECT_COLUMN_INDEX_SCHEMA;
        break;
      case "object_type":
        valueSchema = OBJECT_TYPE_SCHEMA;
        break;
      case "accounting_ledger_account_balance":
        valueSchema = ACCOUNTING_LEDGER_ACCOUNT_BALANCE_SCHEMA;
        break;
      default:
        throw new RuntimeException("Bad table name");
    }

    Envelope createEnvelope =
        Envelope.defineSchema()
            .withName("database-1.admincoin.mutations.Envelope")
            .withRecord(valueSchema)
            .withSource(sourceSchema)
            .build();

    Struct payload = createEnvelope.create(row, source, Instant.now());

    switch (operation) {
      case CREATE:
      case UPDATE:
      case READ:
        payload = createEnvelope.create(row, source, Instant.now());
        break;
      case DELETE:
        payload = createEnvelope.delete(row, source, Instant.now());
        break;
      case TRUNCATE:
        payload = createEnvelope.truncate(source, Instant.now());
        break;
    }

    return new SourceRecord(
        new HashMap<>(),
        new HashMap<>(),
        "database-1.admincoin.mutations",
        createEnvelope.schema(),
        payload);
  }

  private Struct buildObjectRow(String orgGuid) {
    return new Struct(OBJECT_SCHEMA)
        .put("created_at", 1594316988000L)
        .put("creator_guid", "18ff1573-f1d1-415e-ab60-9f58ebdc969d")
        .put("data", ByteBuffer.wrap(new byte[] {1, 2, 3}))
        .put("guid", "63c56787-110e-44c6-8ea8-34b931fecfeb")
        .put("id", 1L)
        .put("object_type", "user")
        .put("organization_guid", orgGuid)
        .put("status", 0)
        .put("updated_at", 1678378814000L)
        .put("version", 1);
  }

  private Struct buildAssociationRow(String sourceOrgGuid, String targetOrgGuid) {
    Struct ret =
        new Struct(ASSOCIATION_SCHEMA)
            .put("association_type_id", 1205)
            .put("created_at", 1709243987000L)
            .put("creator_guid", "18ff1573-f1d1-415e-ab60-9f58ebdc969d")
            .put("id", 334166534L)
            .put("source_guid", "ab273636-e4c7-4e1b-91f7-c702b3e1ce38")
            .put("source_id", 47072302L)
            .put("status", 0)
            .put("target_guid", "5c96d688-9c05-4e03-81a2-1311e0baa85f")
            .put("target_id", 47072328L)
            .put("updated_at", 1709243987000L);

    if (sourceOrgGuid != null)
      ret.put("source_organization_guid", ByteBuffer.wrap(hexStringToByteArray(sourceOrgGuid)));

    if (targetOrgGuid != null)
      ret.put("target_organization_guid", ByteBuffer.wrap(hexStringToByteArray(targetOrgGuid)));

    return ret;
  }

  private Struct buildObjectColumnIndexRow(String orgGuid) {
    return new Struct(OBJECT_COLUMN_INDEX_SCHEMA)
        .put("id", 37L)
        .put("column_name", "status")
        .put("int_value", 0)
        .put("object_guid", "c9ce0f44-ed46-4678-8771-c0cc58652c75")
        .put("object_type", "task")
        .put("organization_guid", orgGuid);
  }

  private Struct buildObjectTypeRow() {
    return new Struct(OBJECT_TYPE_SCHEMA)
        .put("id", 1L)
        .put("object_type_name", "image")
        .put("object_schema", ByteBuffer.wrap(new byte[] {1, 2, 3}))
        .put("version", 3)
        .put("status", 0)
        .put("created_at", 1594316988000L)
        .put("updated_at", 1678378814000L);
  }

  private Struct buildAccountingLedgerAccountBalanceRow(String orgGuid) {
    return new Struct(ACCOUNTING_LEDGER_ACCOUNT_BALANCE_SCHEMA)
        .put("object_id", 421L)
        .put("object_guid", "dbf0ae65-41a4-4e98-aa8b-62b1f068a936")
        .put("organization_guid", orgGuid)
        .put("created_at", 1594316988000L)
        .put("updated_at", 1594316988000L)
        .put("account_number", "0100000000000100158")
        .put("amount_su", 7433L)
        .put("currency", "USD");
  }

  private static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }
}
