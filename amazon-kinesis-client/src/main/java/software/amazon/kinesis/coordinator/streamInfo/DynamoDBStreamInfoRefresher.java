package software.amazon.kinesis.coordinator.streamInfo;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

@Slf4j
@KinesisClientInternalApi
public class DynamoDBStreamInfoRefresher implements StreamInfoRefresher {

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final String currentWorkerId;

    public DynamoDBStreamInfoRefresher(CoordinatorStateDAO coordinatorStateDAO, String currentWorkerId) {
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.currentWorkerId = currentWorkerId;
    }

    @Override
    public boolean createStreamInfo(StreamInfoState streamInfoState)
            throws ProvisionedThroughputException, DependencyException {
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

    @Override
    public boolean deleteStreamInfo(StreamInfoState streamInfoState)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        return coordinatorStateDAO.deleteCoordinatorState(streamInfoState);
    }

    @Override
    public List<StreamInfoState> listStreamInfo() throws ProvisionedThroughputException, DependencyException {
        return coordinatorStateDAO.listCoordinatorStateByEntityType("STREAM").stream()
                .map(state -> (StreamInfoState) state)
                .collect(Collectors.toList());
    }
}
