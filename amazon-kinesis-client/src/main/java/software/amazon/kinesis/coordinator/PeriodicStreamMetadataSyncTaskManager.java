package software.amazon.kinesis.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoRefresher;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoState;
import software.amazon.kinesis.coordinator.streamInfo.StreamMetadataSyncTaskManager;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.metrics.NullMetricsScope;

/**
 * It serves as central orchestrator for stream metadata learning and maintenance.
 * It manages both onboarding of new streams and back-filling of metadata for existing stream.
 */
@Getter
@EqualsAndHashCode
@Slf4j
@KinesisClientInternalApi
public class PeriodicStreamMetadataSyncTaskManager {
    private static final long INITIAL_DELAY = 60 * 1000L;
    private static final String STREAM_METADATA_MANAGER = "PeriodicStreamMetadataSyncTaskManager";
    private boolean isRunning;

    private final LeaderDecider leaderDecider;
    private final ScheduledExecutorService shardSyncThreadPool;
    private final String currentWorkerId;
    private final long delay;
    private final MetricsFactory metricsFactory;
    private final CoordinatorStateDAO coordinatorStateDAO;
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;
    private final boolean isMultiStreamingMode;
    private final StreamInfoRefresher streamInfoRefresher;
    private final Map<StreamConfig, StreamMetadataSyncTaskManager> streamToStreamMetadataSyncManagerMap;
    private final Function<StreamConfig, StreamMetadataSyncTaskManager> streamMetadataSyncManagerProvider;

    /**
     * Pattern for a serialized {@link StreamIdentifier}. The valid format is
     * {@code <accountId>:<streamName>:<creationEpoch>}.
     */
    private static final Pattern STREAM_IDENTIFIER_PATTERN =
            Pattern.compile("(?<accountId>[0-9]+):(?<streamName>[^:]+):(?<creationEpoch>[0-9]+)");

    public PeriodicStreamMetadataSyncTaskManager(
            LeaderDecider leaderDecider,
            String currentWorkerId,
            long delay,
            MetricsFactory metricsFactory,
            CoordinatorStateDAO coordinatorStateDAO,
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            boolean isMultiStreamingMode,
            StreamInfoRefresher streamInfoRefresher,
            Map<StreamConfig, StreamMetadataSyncTaskManager> streamToStreamMetadataSyncManagerMap,
            Function<StreamConfig, StreamMetadataSyncTaskManager> streamMetadataSyncManagerProvider) {
        this.leaderDecider = leaderDecider;
        this.streamInfoRefresher = streamInfoRefresher;
        this.streamToStreamMetadataSyncManagerMap = streamToStreamMetadataSyncManagerMap;
        this.shardSyncThreadPool = Executors.newSingleThreadScheduledExecutor();
        this.currentWorkerId = currentWorkerId;
        this.delay = delay;
        this.metricsFactory = metricsFactory;
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.currentStreamConfigMap = currentStreamConfigMap;
        this.isMultiStreamingMode = isMultiStreamingMode;
        this.streamMetadataSyncManagerProvider = streamMetadataSyncManagerProvider;
    }

    public synchronized TaskResult start() {
        if (!isRunning) {
            final Runnable periodicStreamInfoBackfiller = () -> {
                try {
                    runStreamSync();
                } catch (Throwable t) {
                    log.error("Error during runStreamSync.", t);
                }
            };
            shardSyncThreadPool.scheduleWithFixedDelay(
                    periodicStreamInfoBackfiller, INITIAL_DELAY, delay, TimeUnit.MILLISECONDS);
            isRunning = true;
        }
        return new TaskResult(null);
    }

    public void stop() {
        if (isRunning) {
            log.info(String.format("Shutting down leader decider on worker %s", currentWorkerId));
            leaderDecider.shutdown();
            log.info(String.format("Shutting down periodic shard sync task scheduler on worker %s", currentWorkerId));
            shardSyncThreadPool.shutdown();
            isRunning = false;
        }
    }

    private void runStreamSync() {
        final MetricsScope metricsScope = createMetricsScope(STREAM_METADATA_MANAGER);
        final long startTime = System.currentTimeMillis();
        boolean success = false;

        // If the current worker is not leader, then do nothing as assignment is executed on leader.
        if (!leaderDecider.isLeader(currentWorkerId)) {
            log.debug("WorkerId {} is not a leader, not running the shard sync task", currentWorkerId);
            return;
        }

        log.info(String.format("WorkerId %s is leader, running the periodic shard sync task", currentWorkerId));

        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, STREAM_METADATA_MANAGER);
        int numStreamsWithPartialLeases = 0;
        int numStreamsToSync = 0;
        int numSkippedShardSyncTask = 0;
        boolean isRunSuccess = false;
        final long runStartMillis = System.currentTimeMillis();

        try {
            // list of streams that are owned by workers
            final Set<StreamIdentifier> streamConfigKeys = currentStreamConfigMap.keySet();

            // all streams currently tracked in the metadata table
            List<StreamInfoState> streamInfoStates = streamInfoRefresher.listStreamInfo();

            List<StreamInfoState> needToBackfill = new ArrayList<>();
            // For each of the stream, check if back filling of streamId needs to be done.
            for (StreamIdentifier streamIdentifier : streamConfigKeys) {
                //                StreamInfoState streamInfoState;
                //                if (isMultiStreamingMode) {
                //                    streamInfoState = streamInfoStates.stream()
                //                            .filter(state -> StreamIdentifier.multiStreamInstance(state.getKey())
                //                                    .streamName()
                //                                    .equals(streamIdentifier.streamName()))
                //                            .findFirst()
                //                            .orElse(null);
                //                } else {
                //                    streamInfoState = streamInfoStates.stream()
                //                            .filter(state -> StreamIdentifier.singleStreamInstance(state.getKey())
                //                                    .streamName()
                //                                    .equals(streamIdentifier.streamName()))
                //                            .findFirst()
                //                            .orElse(null);
                //                }
                //                if (streamInfoState == null) {
                //                    log.debug("Stream {} is not present in the metadata table, creating it",
                // streamIdentifier);
                //                    streamInfoState =
                //                            new StreamInfoState(streamIdentifier.serialize(), currentWorkerId, null,
                // "STREAM");
                //                    needToBackfill.add(streamInfoState);
                //                } else {
                //                    log.debug("Stream {} is present in the metadata table", streamIdentifier);
                //                }

                final StreamConfig streamConfig = currentStreamConfigMap.get(streamIdentifier);
                if (streamConfig == null) {
                    log.info("Skipping shard sync task for {} as stream is purged", streamIdentifier);
                    continue;
                }
                final StreamMetadataSyncTaskManager streamMetadataManager;
                if (streamToStreamMetadataSyncManagerMap.containsKey(streamConfig)) {
                    log.info("shardSyncTaskManager for stream {} already exists", streamIdentifier.streamName());
                    streamMetadataManager = streamToStreamMetadataSyncManagerMap.get(streamConfig);
                } else {
                    // If streamConfig of a stream has already been added to currentStreamConfigMap but
                    // Scheduler failed to create shardSyncTaskManager for it, then Scheduler will not try
                    // to create one later. So enable PeriodicShardSyncManager to do it for such cases
                    log.info(
                            "Failed to get shardSyncTaskManager so creating one for stream {}.",
                            streamIdentifier.streamName());
                    streamMetadataManager = streamToStreamMetadataSyncManagerMap.computeIfAbsent(
                            streamConfig, s -> streamMetadataSyncManagerProvider.apply(s));
                }
                if (!streamMetadataManager.submitShardSyncTask()) {
                    log.warn(
                            "Failed to submit stream metadata sync task for stream {}. This could be due to the previous pending shard sync task.",
                            streamIdentifier.streamName());
                    numSkippedShardSyncTask += 1;
                } else {
                    log.info("Submitted stream metadata sync task for stream {}", streamIdentifier.streamName());
                }
            }

            if (needToBackfill == null || needToBackfill.size() == 0) {
                log.debug("No stream info needs to be back filled in this run");
            }

            isRunSuccess = true;
        } catch (Exception e) {
            log.error("Caught exception while syncing streamId.", e);
        } finally {
            ///  todo content from PeriodicShardSyncManager runShardSync method
            scope.addData(
                    "NumStreamsWithPartialLeases",
                    numStreamsWithPartialLeases,
                    StandardUnit.COUNT,
                    MetricsLevel.SUMMARY);
            MetricsUtil.addSuccessAndLatency(scope, isRunSuccess, runStartMillis, MetricsLevel.SUMMARY);
            scope.end();
        }
    }

    /**
     * Creates the MetricsScope for given {@param operation} by calling metricsFactory and falls back to
     * NullMetricsScope if failed to create MetricsScope.
     * @param operation Operation name for MetricsScope
     * @return instance of MetricsScope
     */
    private MetricsScope createMetricsScope(final String operation) {
        try {
            return MetricsUtil.createMetricsWithOperation(metricsFactory, operation);
        } catch (final Exception e) {
            log.error("Failed to create metrics scope defaulting to no metrics.", e);
            return new NullMetricsScope();
        }
    }
}
