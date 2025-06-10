package software.amazon.kinesis.coordinator.streamInfo;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.ExecutorStateEvent;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.MetricsCollectingTaskDecorator;
import software.amazon.kinesis.metrics.MetricsFactory;

@Data
@Accessors(fluent = true)
@Slf4j
@KinesisClientInternalApi
public class StreamMetadataSyncTaskManager {
    @NonNull
    private final ExecutorService executorService;

    @NonNull
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;

    @NonNull
    private final StreamInfoRefresher streamInfoRefresher;

    @NonNull
    private final MetricsFactory metricsFactory;

    private final boolean isMultiStreamMode;

    private final StreamConfig streamConfig;

    private ConsumerTask currentTask;
    private CompletableFuture<TaskResult> future;
    private AtomicBoolean streamMetadataSyncRequestPending;
    private final ReentrantLock lock;

    public StreamMetadataSyncTaskManager(
            @NonNull Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            @NotNull StreamInfoRefresher streamInfoRefresher,
            @NonNull MetricsFactory metricsFactory,
            boolean isMultiStreamMode,
            StreamConfig streamConfig) {
        this.currentStreamConfigMap = currentStreamConfigMap;
        this.isMultiStreamMode = isMultiStreamMode;
        this.streamConfig = streamConfig;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.streamInfoRefresher = streamInfoRefresher;
        this.metricsFactory = metricsFactory;
        this.lock = new ReentrantLock();
    }

    /**
     * Submit a ShardSyncTask and return if the submission is successful.
     * @return if the casting is successful.
     */
    public boolean submitShardSyncTask() {
        try {
            lock.lock();
            return checkAndSubmitNextTask();
        } finally {
            lock.unlock();
        }
    }

    private boolean checkAndSubmitNextTask() {
        boolean submittedNewTask = false;
        if ((future == null) || future.isCancelled() || future.isDone()) {
            if ((future != null) && future.isDone()) {
                try {
                    TaskResult result = future.get();
                    if (result.getException() != null) {
                        log.error("Caught exception running {} task: ", currentTask.taskType(), result.getException());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("{} task encountered exception.", currentTask.taskType(), e);
                }
            }

            currentTask = new MetricsCollectingTaskDecorator(
                    new StreamMetadataSyncTask(
                            currentStreamConfigMap,
                            streamInfoRefresher,
                            metricsFactory,
                            isMultiStreamMode,
                            streamConfig.streamIdentifier()),
                    metricsFactory);
            future = CompletableFuture.supplyAsync(() -> currentTask.call(), executorService)
                    .whenComplete((taskResult, exception) -> handlePendingShardSyncs(exception, taskResult));

            log.info(new ExecutorStateEvent(executorService).message());

            submittedNewTask = true;
            if (log.isDebugEnabled()) {
                log.debug("Submitted new {} task.", currentTask.taskType());
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Previous {} task still pending.  Not submitting new task. "
                                + "Triggered a pending request but will not be executed until the current request completes.",
                        currentTask.taskType());
            }
            streamMetadataSyncRequestPending.compareAndSet(false /*expected*/, true /*update*/);
        }
        return submittedNewTask;
    }

    private void handlePendingShardSyncs(Throwable exception, TaskResult taskResult) {
        if (exception != null || taskResult.getException() != null) {
            log.error(
                    "Caught exception running {} task: {}",
                    currentTask.taskType(),
                    exception != null ? exception : taskResult.getException());
        }
    }
}
