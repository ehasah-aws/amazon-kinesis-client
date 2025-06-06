package software.amazon.kinesis.coordinator.streamInfo;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;

/**
 * Data model of the StreamInfo state. This is used to track StreamMetadata
 */
@Getter
@ToString(callSuper = true)
@Slf4j
@KinesisClientInternalApi
public class StreamInfoState extends CoordinatorState {

    public static final String MODIFIED_BY_ATTRIBUTE_NAME = "mb";
    public static final String MODIFIED_TIMESTAMP_ATTRIBUTE_NAME = "mts";
    public static final String STREAM_ID_ATTRIBUTE_NAME = "streamId";
    public static final String ENTITY_TYPE_ATTRIBUTE_NAME = "entityType";

    private String modifiedBy;
    private long modifiedTimestamp;
    private String streamId;

    private StreamInfoState(
            final String key,
            final String modifiedBy,
            final long modifiedTimestamp,
            final String streamId,
            final String entityType,
            final Map<String, AttributeValue> others) {
        setKey(key);
        setAttributes(others);
        this.modifiedBy = modifiedBy;
        this.modifiedTimestamp = modifiedTimestamp;
        this.streamId = streamId;
        setEntityType(entityType);
    }

    public StreamInfoState(final String key, final String modifiedBy, String streamId, String entityType) {
        this(key, modifiedBy, System.currentTimeMillis(), streamId, entityType, new HashMap<>());
    }

    public StreamInfoState copy() {
        return new StreamInfoState(
                getKey(),
                getModifiedBy(),
                getModifiedTimestamp(),
                getStreamId(),
                getEntityType(),
                new HashMap<>(getAttributes()));
    }

    public HashMap<String, AttributeValue> serialize() {
        final HashMap<String, AttributeValue> result = new HashMap<>();
        result.put(MODIFIED_BY_ATTRIBUTE_NAME, AttributeValue.fromS(modifiedBy));
        result.put(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN(String.valueOf(modifiedTimestamp)));
        result.put(STREAM_ID_ATTRIBUTE_NAME, AttributeValue.fromN(String.valueOf(streamId)));
        result.put(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME, AttributeValue.fromN(String.valueOf(modifiedTimestamp)));
        return result;
    }

    public static StreamInfoState deserialize(final String key, final HashMap<String, AttributeValue> attributes) {
        try {
            final HashMap<String, AttributeValue> mutableAttributes = new HashMap<>(attributes);
            final String modifiedBy =
                    mutableAttributes.remove(MODIFIED_BY_ATTRIBUTE_NAME).s();
            final long modifiedTimestamp = Long.parseLong(
                    mutableAttributes.remove(MODIFIED_TIMESTAMP_ATTRIBUTE_NAME).n());
            final String streamId =
                    mutableAttributes.remove(STREAM_ID_ATTRIBUTE_NAME).s();
            final String entityType =
                    mutableAttributes.remove(ENTITY_TYPE_ATTRIBUTE_NAME).s();

            final StreamInfoState migrationState =
                    new StreamInfoState(key, modifiedBy, modifiedTimestamp, streamId, entityType, mutableAttributes);

            if (!mutableAttributes.isEmpty()) {
                log.info("Unknown attributes {} for state {}", mutableAttributes, migrationState);
            }
            return migrationState;

        } catch (final Exception e) {
            log.warn("Unable to deserialize state with key {} and attributes {}", key, attributes, e);
        }
        return null;
    }

    public Map<String, AttributeValueUpdate> getDynamoUpdate() {
        return new HashMap<>();
    }
}
