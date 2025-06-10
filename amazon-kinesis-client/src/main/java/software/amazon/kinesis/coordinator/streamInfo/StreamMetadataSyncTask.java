package software.amazon.kinesis.coordinator.streamInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class StreamMetadataSyncTask implements ConsumerTask {
    private static final String STREAM_METADATA_SYNC_TASK_OPERATION = "StreamMetadataSyncTask";

    @NonNull
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;

    @NonNull
    private final StreamInfoRefresher streamInfoRefresher;

    @NonNull
    private final MetricsFactory metricsFactory;

    private final boolean isMultiStreamMode;

    private final StreamIdentifier streamIdentifier;

    private final TaskType taskType = TaskType.STREAM_METADATA_SYNC;

    @Override
    public TaskResult call() {
        Exception exception = null;
        final MetricsScope scope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, STREAM_METADATA_SYNC_TASK_OPERATION);
        boolean streamMetadataSuccess = true;

        // all streams currently tracked in the metadata table
        try {
            final Set<StreamIdentifier> streamConfigKeys = currentStreamConfigMap.keySet();
            List<StreamInfoState> streamInfoStates = streamInfoRefresher.listStreamInfo();
            final Set<StreamInfoState> createdStreamInfoState = new HashSet<>();
            try {
                StreamInfoState streamInfoState;
                if (isMultiStreamMode) {
                    streamInfoState = streamInfoStates.stream()
                            .filter(state -> StreamIdentifier.multiStreamInstance(state.getKey())
                                    .streamName()
                                    .equals(streamIdentifier.streamName()))
                            .findFirst()
                            .orElse(null);
                } else {
                    streamInfoState = streamInfoStates.stream()
                            .filter(state -> StreamIdentifier.singleStreamInstance(state.getKey())
                                    .streamName()
                                    .equals(streamIdentifier.streamName()))
                            .findFirst()
                            .orElse(null);
                }
                streamInfoRefresher.createStreamInfo(streamInfoState);
                createdStreamInfoState.add(streamInfoState);
            } catch (Exception e) {
                log.error("Caught exception while creating stream metadata info.", e);
                exception = e;
                streamMetadataSuccess = false;
            } finally {
                MetricsUtil.addSuccess(scope, "StreamMetadataSync", streamMetadataSuccess, MetricsLevel.DETAILED);
                MetricsUtil.endScope(scope);
            }
        } catch (ProvisionedThroughputException e) {
            throw new RuntimeException(e);
        } catch (DependencyException e) {
            throw new RuntimeException(e);
        }

        return new TaskResult(exception);
    }

    @Override
    public TaskType taskType() {
        return taskType;
    }
}
