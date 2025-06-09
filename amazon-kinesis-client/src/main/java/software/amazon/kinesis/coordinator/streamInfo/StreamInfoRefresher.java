package software.amazon.kinesis.coordinator.streamInfo;

import java.util.List;

import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * Supports basic CRUD operations for StreamInfo.
 */
public interface StreamInfoRefresher {

    /*
     * createStreamInfo
     * deleteStreamInfo
     * updateStreamInfo
     * listStreamInfo
     *
     * */

    boolean createStreamInfo(StreamInfoState streamInfoState)
            throws ProvisionedThroughputException, DependencyException;

    boolean deleteStreamInfo(StreamIdentifier streamIdentifier)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException;

    boolean updateStreamInfo(StreamInfoState streamInfoState)
            throws ProvisionedThroughputException, DependencyException;

    List<StreamInfoState> listStreamInfo(StreamIdentifier streamIdentifier)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException;
}
