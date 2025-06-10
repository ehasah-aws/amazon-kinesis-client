package software.amazon.kinesis.coordinator.streamInfo;

import java.util.List;

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * Supports basic CRUD operations for StreamInfo.
 */
public interface StreamInfoRefresher {

    boolean createStreamInfo(StreamInfoState streamInfoState)
            throws ProvisionedThroughputException, DependencyException;

    boolean deleteStreamInfo(StreamInfoState coordinatorState)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException;

    List<StreamInfoState> listStreamInfo() throws ProvisionedThroughputException, DependencyException;
}
