package com.ziphq.kafka.connect.smt;

import static io.debezium.data.Envelope.FieldName.*;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;
import io.debezium.util.MurmurHash3;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This SMT allow to use payload fields to calculate the destination partition.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Mario Fiore Vitale
 */
public class PartitionRouting<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String FIELD_TOPIC_PARTITION_NUM_CONF = "partition.topic.num";
  public static final String FIELD_HASH_FUNCTION = "partition.hash.function";
  static final Field TOPIC_PARTITION_NUM_FIELD =
      Field.create(FIELD_TOPIC_PARTITION_NUM_CONF)
          .withDisplayName("Number of partition configured for topic")
          .withType(ConfigDef.Type.INT)
          .withValidation(Field::isPositiveInteger)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDescription(
              "Number of partition for the topic on which this SMT act. Use TopicNameMatches"
                  + " predicate to filter records by topic")
          .required();
  static final Field HASH_FUNCTION_FIELD =
      Field.create(FIELD_HASH_FUNCTION)
          .withDisplayName("Hash function")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Hash function to be used when computing hash of the fields which would determine"
                  + " number of the destination partition.")
          .withDefault("java")
          .optional();
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionRouting.class);
  private SmtManager<R> smtManager;
  private int partitionNumber;
  private HashFunction hashFc;

  @Override
  public ConfigDef config() {

    ConfigDef config = new ConfigDef();
    // group does not manage validator definition. Validation will not work here.
    return Field.group(config, "partitions", TOPIC_PARTITION_NUM_FIELD, HASH_FUNCTION_FIELD);
  }

  @Override
  public void configure(Map<String, ?> props) {

    final Configuration config = Configuration.from(props);

    smtManager = new SmtManager<>(config);

    smtManager.validate(config, Field.setOf(TOPIC_PARTITION_NUM_FIELD));

    partitionNumber = config.getInteger(TOPIC_PARTITION_NUM_FIELD);
    hashFc = HashFunction.parse(config.getString(HASH_FUNCTION_FIELD));
  }

  @Override
  public R apply(R originalRecord) {

    if (originalRecord.value() == null || !smtManager.isValidEnvelope(originalRecord)) {
      LOGGER.trace("Skipping tombstone or message without envelope");
      return originalRecord;
    }

    final Struct envelope = (Struct) originalRecord.value();
    try {
      if (SmtManager.isGenericOrTruncateMessage((SourceRecord) originalRecord)) {
        LOGGER.trace("Skip generic or truncate messages");
        return originalRecord;
      }

      Optional<String> organizationGuid = getOrganizationGuid(envelope);
      if (organizationGuid.isEmpty()) {
        return originalRecord;
      }

      int partition = calPartition(partitionNumber, organizationGuid.get());
      return buildNewRecord(originalRecord, envelope, partition);
    } catch (Exception e) {
      LOGGER.error("Fail to process event {}", originalRecord, e);
      return originalRecord;
    }
  }

  static Optional<String> getOrganizationGuid(Struct envelope) {
    try {
      Struct source = envelope.getStruct(SOURCE);
      String tableName = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);

      Envelope.Operation operation = Envelope.Operation.forCode(envelope.getString(OPERATION));
      String beforeOrAfter = Envelope.Operation.DELETE.equals(operation) ? BEFORE : AFTER;

      String orgGuid = null;
      if (tableName.equals("object")) {
        Struct s = envelope.getStruct(beforeOrAfter);
        orgGuid = s.getString("organization_guid");
      } else if (tableName.equals("association")) {
        Struct s = envelope.getStruct(beforeOrAfter);
        byte[] sourceOrgGuid = s.getBytes("source_organization_guid");
        byte[] targetOrgGuid = s.getBytes("target_organization_guid");
        if (sourceOrgGuid != null) {
          orgGuid = convertBytesToUUID(sourceOrgGuid);
        } else if (targetOrgGuid != null) {
          orgGuid = convertBytesToUUID(targetOrgGuid);
        }
      } else {
        // This includes
        // 1. object_column_index table => has organization_guid field
        // 2. broken out tables => has organization_guid field
        // 3. other tables such as object_type => does not have organization_guid field
        Struct s = envelope.getStruct(beforeOrAfter);
        org.apache.kafka.connect.data.Field orgGuidField = s.schema().field("organization_guid");
        if (orgGuidField != null) {
          if (orgGuidField.schema().type() == Schema.Type.STRING) {
            orgGuid = s.getString("organization_guid");
          } else if (orgGuidField.schema().type() == Schema.Type.BYTES) {
            byte[] bs = s.getBytes("organization_guid");
            if (bs != null) {
              orgGuid = convertBytesToUUID(bs);
            }
          } else {
            LOGGER.error("Table {} has wrong type for organization_guid field.", tableName);
          }
        }
      }

      return Optional.ofNullable(orgGuid);
    } catch (DataException e) {
      LOGGER.info("Fail to extract organization guid in payload {}.", envelope);
      return Optional.empty();
    }
  }

  private R buildNewRecord(R originalRecord, Struct envelope, int partition) {
    LOGGER.trace("Message {} will be sent to partition {}", envelope, partition);

    return originalRecord.newRecord(
        originalRecord.topic(),
        partition,
        originalRecord.keySchema(),
        originalRecord.key(),
        originalRecord.valueSchema(),
        envelope,
        originalRecord.timestamp(),
        originalRecord.headers());
  }

  protected int calPartition(Integer partitionNumber, String orgGuid) {
    int hashCode = hashFc.getHash().apply(orgGuid);
    // hashCode can be negative due to overflow. Since Math.abs(Integer.MIN_INT) will still return a
    // negative number
    // we use bitwise operation to remove the sign
    int normalizedHash = hashCode & Integer.MAX_VALUE;
    if (normalizedHash == Integer.MAX_VALUE) {
      normalizedHash = 0;
    }
    return normalizedHash % partitionNumber;
  }

  static String convertBytesToUUID(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    long high = byteBuffer.getLong();
    long low = byteBuffer.getLong();
    return new UUID(high, low).toString();
  }

  @Override
  public void close() {}

  public enum HashFunction implements EnumeratedValue {
    /** Hash function to be used when computing hash of the fields of the record. */
    JAVA("java", Object::hashCode),
    MURMUR("murmur", MurmurHash3.getInstance()::hash);

    private final String name;
    private final Function<Object, Integer> hash;

    HashFunction(String value, Function<Object, Integer> hash) {
      this.name = value;
      this.hash = hash;
    }

    public static HashFunction parse(String value) {
      if (value == null) {
        return JAVA;
      }
      value = value.trim().toLowerCase();
      for (HashFunction option : HashFunction.values()) {
        if (option.getValue().equalsIgnoreCase(value)) {
          return option;
        }
      }
      return JAVA;
    }

    @Override
    public String getValue() {
      return name;
    }

    public Function<Object, Integer> getHash() {
      return hash;
    }
  }
}
