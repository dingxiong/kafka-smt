package com.ziphq.kafka.connect.smt;


import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;
import io.debezium.util.MurmurHash3;
import io.debezium.connector.AbstractSourceInfo;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;


import static io.debezium.data.Envelope.FieldName.*;

/**
 * This SMT allow to use payload fields to calculate the destination partition.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Mario Fiore Vitale
 */
public class PartitionRouting<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_PAYLOAD_FIELD_CONF = "partition.payload.fields";
    public static final String FIELD_TOPIC_PARTITION_NUM_CONF = "partition.topic.num";
    public static final String FIELD_HASH_FUNCTION = "partition.hash.function";
    static final Field PARTITION_PAYLOAD_FIELDS_FIELD = Field.create(FIELD_PAYLOAD_FIELD_CONF)
            .withDisplayName("List of payload fields to use for compute partition.")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(
                    Field::notContainEmptyElements)
            .withDescription("Payload fields to use to calculate the partition. Supports Struct nesting using dot notation." +
                    "To access fields related to data collections, you can use: after, before or change, " +
                    "where 'change' is a special field that will automatically choose, based on operation, the 'after' or 'before'. " +
                    "If a field not exist for the current record it will simply not used" +
                    "e.g. after.name,source.table,change.name")
            .required();
    static final Field TOPIC_PARTITION_NUM_FIELD = Field.create(FIELD_TOPIC_PARTITION_NUM_CONF)
            .withDisplayName("Number of partition configured for topic")
            .withType(ConfigDef.Type.INT)
            .withValidation(Field::isPositiveInteger)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Number of partition for the topic on which this SMT act. Use TopicNameMatches predicate to filter records by topic")
            .required();
    static final Field HASH_FUNCTION_FIELD = Field.create(FIELD_HASH_FUNCTION)
            .withDisplayName("Hash function")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Hash function to be used when computing hash of the fields which would determine number of the destination partition.")
            .withDefault("java")
            .optional();
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionRouting.class);
    private static final MurmurHash3 MURMUR_HASH_3 = MurmurHash3.getInstance();
    private SmtManager<R> smtManager;
    private List<String> payloadFields;
    private int partitionNumber;
    private HashFunction hashFc;


    @Override
    public ConfigDef config() {

        ConfigDef config = new ConfigDef();
        // group does not manage validator definition. Validation will not work here.
        return Field.group(config, "partitions",
                PARTITION_PAYLOAD_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD, HASH_FUNCTION_FIELD);
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);

        smtManager = new SmtManager<>(config);

        smtManager.validate(config, Field.setOf(PARTITION_PAYLOAD_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD));

        payloadFields = config.getList(PARTITION_PAYLOAD_FIELDS_FIELD);
        partitionNumber = config.getInteger(TOPIC_PARTITION_NUM_FIELD);
        hashFc = HashFunction.parse(config.getString(HASH_FUNCTION_FIELD));
    }

    @Override
    public R apply(R originalRecord) {
        LOGGER.info("xxxx {}", originalRecord);

        if (originalRecord.value() == null || !smtManager.isValidEnvelope(originalRecord)) {
            LOGGER.info("Skipping tombstone or message without envelope");
            return originalRecord;
        }

        final Struct envelope = (Struct) originalRecord.value();
        try {
            if (SmtManager.isGenericOrTruncateMessage((SourceRecord) originalRecord)) {
                LOGGER.info("Skip generic or truncate messages");
                return originalRecord;
            }
            LOGGER.info("xxxx envelope {}", envelope);

            Optional<String> organizationGuid = getOrganizationGuid(envelope);
            LOGGER.info("xxx org guid {}", organizationGuid);
            System.out.println("xxx org guid " + organizationGuid);
            if (organizationGuid.isEmpty()) {
                return originalRecord;
            }

            int partition = calPartition(partitionNumber, organizationGuid.get());
            LOGGER.info("xxx partition {} {}", partition, partitionNumber);
            System.out.println("xxx partition " + partition + " " + partitionNumber);
            return buildNewRecord(originalRecord, envelope, partition);
        } catch (Exception e) {
            // throw new DebeziumException(String.format("Unprocessable message %s", envelope), e);
            LOGGER.error("{}", e);
            return originalRecord;
        }
    }

    static Optional<String> getOrganizationGuid(Struct envelope) {
        try {
            Struct source = envelope.getStruct(SOURCE);
            String table_name = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);

            Envelope.Operation operation = Envelope.Operation.forCode(envelope.getString(OPERATION));
            String before_or_after = Envelope.Operation.DELETE.equals(operation) ? BEFORE : AFTER;

            String org_guid = null;
            if (table_name.equals("object")) {
                Struct s = envelope.getStruct(before_or_after);
                org_guid = s.getString("organization_guid");
            } else if (table_name.equals("association")) {
                Struct s = envelope.getStruct(before_or_after);
                byte[] source_org_guid = s.getBytes("source_organization_guid");
                byte[] target_org_guid = s.getBytes("target_organization_guid");
                if (source_org_guid != null) {
                    org_guid = convertBytesToUUID(source_org_guid);
                } else if (target_org_guid != null) {
                    org_guid =  convertBytesToUUID(target_org_guid);
                }
            } else {
                // This includes
                // 1. object_column_index table => has organization_guid field
                // 2. broken out tables => has organization_guid field
                // 3. other tables such as object_type => does not have organization_guid field
                Struct s = envelope.getStruct(before_or_after);
                org.apache.kafka.connect.data.Field org_guid_field = s.schema().field("organization_guid");
                if (org_guid_field != null) {
                    if (org_guid_field.schema().type() == Schema.Type.STRING) {
                        org_guid = s.getString("organization_guid");
                    } else if (org_guid_field.schema().type() == Schema.Type.BYTES) {
                        byte[] bs = s.getBytes("organization_guid");
                        if (bs != null) {
                            org_guid = convertBytesToUUID(bs);
                        }
                    } else {
                        LOGGER.error("Table {} has wrong type for organization_guid field.", table_name);
                    }
                }
            }

            return Optional.ofNullable(org_guid);
        } catch (DataException e) {
            LOGGER.info("Fail to extract organization guid in payload {}. It will not be considered", envelope);
            return Optional.empty();
        }
    }

    private R buildNewRecord(R originalRecord, Struct envelope, int partition) {
        LOGGER.trace("Message {} will be sent to partition {}", envelope, partition);

        return originalRecord.newRecord(originalRecord.topic(), partition,
                originalRecord.keySchema(),
                originalRecord.key(),
                originalRecord.valueSchema(),
                envelope,
                originalRecord.timestamp(),
                originalRecord.headers());
    }

    protected int calPartition(Integer partitionNumber, String orgGuid) {
        int hashCode = hashFc.getHash().apply(orgGuid);
        // hashCode can be negative due to overflow. Since Math.abs(Integer.MIN_INT) will still return a negative number
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
    public void close() {
    }

    public enum HashFunction implements EnumeratedValue {
        /**
         * Hash function to be used when computing hash of the fields of the record.
         */

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

