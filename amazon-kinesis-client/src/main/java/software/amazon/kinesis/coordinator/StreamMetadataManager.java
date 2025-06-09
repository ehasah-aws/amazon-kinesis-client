package software.amazon.kinesis.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoRefresher;
import software.amazon.kinesis.coordinator.streamInfo.StreamInfoState;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.MetricsFactory;
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
public class StreamMetadataManager {
    private static final long INITIAL_DELAY = 60 * 1000L;
    private static final String STREAM_METADATA_MANAGER = "StreamMetadataManager";
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

    /**
     * Pattern for a serialized {@link StreamIdentifier}. The valid format is
     * {@code <accountId>:<streamName>:<creationEpoch>}.
     */
    private static final Pattern STREAM_IDENTIFIER_PATTERN =
            Pattern.compile("(?<accountId>[0-9]+):(?<streamName>[^:]+):(?<creationEpoch>[0-9]+)");

    public StreamMetadataManager(
            LeaderDecider leaderDecider,
            String currentWorkerId,
            long delay,
            MetricsFactory metricsFactory,
            CoordinatorStateDAO coordinatorStateDAO,
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            boolean isMultiStreamingMode,
            StreamInfoRefresher streamInfoRefresher) {
        this.leaderDecider = leaderDecider;
        this.streamInfoRefresher = streamInfoRefresher;
        this.shardSyncThreadPool = Executors.newSingleThreadScheduledExecutor();
        this.currentWorkerId = currentWorkerId;
        this.delay = delay;
        this.metricsFactory = metricsFactory;
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.currentStreamConfigMap = currentStreamConfigMap;
        this.isMultiStreamingMode = isMultiStreamingMode;
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
            List<StreamInfoState> streamInfoStates =
                    coordinatorStateDAO.listCoordinatorStateByEntityType("STREAM").stream()
                            .map(state -> (StreamInfoState) state)
                            .collect(Collectors.toList());
            List<StreamIdentifier> needToBackfill = new ArrayList<>();
            // For each of the stream, check if back filling of streamId needs to be done.
            for (StreamIdentifier streamIdentifier : streamConfigKeys) {
                StreamInfoState streamInfoState;
                if (isMultiStreamingMode) {
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
                if (streamInfoState == null) {
                    log.debug("Stream {} is not present in the metadata table, creating it", streamIdentifier);
                    needToBackfill.add(streamIdentifier);
                    streamInfoState =
                            new StreamInfoState(streamIdentifier.serialize(), currentWorkerId, null, "STREAM");
                    final boolean created = createStreamInfo(streamInfoState);
                    if (!created) {
                        log.debug("Create {} did not succeed", streamInfoState);
                    }
                } else {
                    log.debug("Stream {} is present in the metadata table", streamIdentifier);
                }
            }

            log.info("List of stream config are: " + streamConfigKeys);
        } catch (Exception e) {
            log.error("Caught exception while syncing streamId.", e);
        } finally {
            ///  todo content from PeriodicShardSyncManager runShardSync method
        }
    }

    /**
     * Enqueues creation of new stream metadata. Ensures stream is added to coordinator table
     * before leases can be created.
     * We want to ensure that starting from now the caller doesn’t create leases
     * before stream in added to the coordinator table already.
     *
     * @param streamInfoState The identifier of the stream to create metadata for
     * @return true if stream was created, false if stream already exists
     */
    public boolean createStreamInfo(StreamInfoState streamInfoState) {
        try {
            return create(streamInfoState)
                    .thenApply(created -> {
                        if (created) {
                            log.info("Stream was created successfully");
                        } else {
                            log.info("Stream already exists");
                        }
                        return created;
                    })
                    .join();
        } catch (Exception e) {
            log.error("Failed to create stream", e);
            return false;
        }
    }

    private CompletableFuture<Boolean> create(final StreamInfoState streamInfoState) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return coordinatorStateDAO.createCoordinatorStateIfNotExists(streamInfoState);
            } catch (Exception e) {
                log.error("Failed to create stream metadata", e);
                throw new RuntimeException("Failed to create stream metadata", e);
            }
        });
    }

    public void delete(StreamIdentifier streamIdentifier)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        coordinatorStateDAO.deleteCoordinatorState(
                new StreamInfoState(streamIdentifier.serialize(), currentWorkerId, null, "STREAM"));
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
