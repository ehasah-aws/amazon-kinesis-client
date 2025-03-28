package software.amazon.kinesis.worker.metricstats;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.internal.waiters.DefaultWaiterResponse;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.dynamodb.model.BillingMode.PROVISIONED;
import static software.amazon.kinesis.common.FutureUtils.unwrappingFuture;

class WorkerMetricsDAOTest {

    private static final String TEST_WORKER_METRICS_TABLE = "WorkerMetricsTableTest";
    private static final Long TEST_REPORTER_FREQ_MILLIS = 10_000L;
    private static final String TEST_WORKER_ID = "TEST_WORKER_ID";
    private final DynamoDbAsyncClient dynamoDbAsyncClient =
            DynamoDBEmbedded.create().dynamoDbAsyncClient();
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient = DynamoDbEnhancedAsyncClient.builder()
            .dynamoDbClient(dynamoDbAsyncClient)
            .build();
    private final DynamoDbAsyncTable<WorkerMetricStats> workerMetricsTable =
            dynamoDbEnhancedAsyncClient.table(TEST_WORKER_METRICS_TABLE, TableSchema.fromBean(WorkerMetricStats.class));
    private WorkerMetricStatsDAO workerMetricsDAO;

    private void setUp() {
        final WorkerMetricsTableConfig tableConfig = new WorkerMetricsTableConfig(null);
        tableConfig.tableName(TEST_WORKER_METRICS_TABLE);
        this.workerMetricsDAO = setUp(tableConfig, this.dynamoDbAsyncClient);
    }

    private WorkerMetricStatsDAO setUp(
            final WorkerMetricsTableConfig workerMetricsTableConfig, final DynamoDbAsyncClient dynamoDbAsyncClient) {
        final WorkerMetricStatsDAO dao =
                new WorkerMetricStatsDAO(dynamoDbAsyncClient, workerMetricsTableConfig, TEST_REPORTER_FREQ_MILLIS);
        assertDoesNotThrow(dao::initialize);
        return dao;
    }

    @Test
    void initialize_sanity() {
        setUp();
        final DescribeTableResponse describeTableResponse =
                unwrappingFuture(() -> dynamoDbAsyncClient.describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_WORKER_METRICS_TABLE)
                        .build()));
        assertEquals(describeTableResponse.table().tableStatus(), TableStatus.ACTIVE, "Table status is not ACTIVE");
        assertFalse(describeTableResponse.table().deletionProtectionEnabled());
    }

    @Test
    void initialize_withDeletionProtection_assertDeletionProtection() {
        final WorkerMetricsTableConfig config = new WorkerMetricsTableConfig(null);
        config.tableName(TEST_WORKER_METRICS_TABLE);
        config.deletionProtectionEnabled(true);
        setUp(config, dynamoDbAsyncClient);
        final DescribeTableResponse describeTableResponse =
                unwrappingFuture(() -> dynamoDbAsyncClient.describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_WORKER_METRICS_TABLE)
                        .build()));

        assertTrue(describeTableResponse.table().deletionProtectionEnabled());
    }

    /**
     * DynamoDBLocal does not support PITR and tags and thus this test is using mocks.
     */
    @Test
    void initialize_withTagAndPitr_assertCall() {
        final DynamoDbAsyncWaiter waiter = mock(DynamoDbAsyncWaiter.class);
        final WaiterResponse<?> waiterResponse = DefaultWaiterResponse.builder()
                .response(dummyDescribeTableResponse(TableStatus.ACTIVE))
                .attemptsExecuted(1)
                .build();
        when(waiter.waitUntilTableExists(any(Consumer.class), any(Consumer.class)))
                .thenReturn(CompletableFuture.completedFuture((WaiterResponse<DescribeTableResponse>) waiterResponse));

        final DynamoDbAsyncClient dbAsyncClient = mock(DynamoDbAsyncClient.class);
        when(dbAsyncClient.waiter()).thenReturn(waiter);
        when(dbAsyncClient.createTable(any(CreateTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(dbAsyncClient.updateContinuousBackups(any(UpdateContinuousBackupsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(dbAsyncClient.describeTable(any(DescribeTableRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().build())
                .thenReturn(CompletableFuture.completedFuture(dummyDescribeTableResponse(TableStatus.CREATING)))
                .thenReturn(CompletableFuture.completedFuture(dummyDescribeTableResponse(TableStatus.ACTIVE)));

        final ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor =
                ArgumentCaptor.forClass(CreateTableRequest.class);
        final ArgumentCaptor<UpdateContinuousBackupsRequest> updateContinuousBackupsRequestArgumentCaptor =
                ArgumentCaptor.forClass(UpdateContinuousBackupsRequest.class);

        final WorkerMetricsTableConfig config = new WorkerMetricsTableConfig(null);
        config.tableName(TEST_WORKER_METRICS_TABLE);
        config.pointInTimeRecoveryEnabled(true);
        config.tags(
                Collections.singleton(Tag.builder().key("Key").value("Value").build()));
        setUp(config, dbAsyncClient);

        verify(dbAsyncClient).createTable(createTableRequestArgumentCaptor.capture());
        verify(dbAsyncClient).updateContinuousBackups(updateContinuousBackupsRequestArgumentCaptor.capture());

        assertEquals(1, createTableRequestArgumentCaptor.getValue().tags().size());
        assertEquals(
                "Key", createTableRequestArgumentCaptor.getValue().tags().get(0).key());
        assertEquals(
                "Value",
                createTableRequestArgumentCaptor.getValue().tags().get(0).value());

        assertTrue(updateContinuousBackupsRequestArgumentCaptor
                .getAllValues()
                .get(0)
                .pointInTimeRecoverySpecification()
                .pointInTimeRecoveryEnabled());
    }

    private static DescribeTableResponse dummyDescribeTableResponse(final TableStatus tableStatus) {
        return DescribeTableResponse.builder()
                .table(TableDescription.builder().tableStatus(tableStatus).build())
                .build();
    }

    @Test
    void updateStats_sanity() {
        setUp();
        final WorkerMetricStats workerMetrics = createDummyWorkerMetrics(TEST_WORKER_ID);
        workerMetrics.setOperatingRange(ImmutableMap.of("C", ImmutableList.of(100L)));
        workerMetricsDAO.updateMetrics(workerMetrics);

        final WorkerMetricStats response1 = getWorkerMetricFromTable(TEST_WORKER_ID);

        assertEquals(workerMetrics, response1, "WorkerMetricStats entry from storage is not matching");

        final WorkerMetricStats workerMetricsUpdated = createDummyWorkerMetrics(TEST_WORKER_ID);
        // Don't update lastUpdateTime
        workerMetricsUpdated.setOperatingRange(null);
        workerMetricsUpdated.setMetricStats(ImmutableMap.of("M", ImmutableList.of(10D, 12D)));

        workerMetricsDAO.updateMetrics(workerMetricsUpdated);

        final WorkerMetricStats response2 = getWorkerMetricFromTable(TEST_WORKER_ID);

        // assert lastUpdateTime is unchanged.
        assertEquals(
                response1.getOperatingRange(), response2.getOperatingRange(), "lastUpdateTime attribute is not equal");
        assertNotEquals(
                response1.getMetricStats(),
                response2.getMetricStats(),
                "ResourcesStats attribute is equal wanted unequal");
    }

    @Test
    void updateStats_withEmptyStatValue_throwIllegalArgumentException() {
        setUp();
        final WorkerMetricStats workerMetrics = createDummyWorkerMetrics(TEST_WORKER_ID);
        workerMetrics.setMetricStats(ImmutableMap.of("C", Collections.emptyList()));

        assertThrows(
                IllegalArgumentException.class,
                () -> workerMetricsDAO.updateMetrics(workerMetrics),
                "Validation on empty stats values for workerMetric did not fail with IllegalArgumentException");
    }

    @Test
    void updateStats_withUpdateTimeOlderThanAllowed_throwIllegalArgumentException() {
        setUp();
        final WorkerMetricStats workerMetrics = createDummyWorkerMetrics(TEST_WORKER_ID);
        workerMetrics.setLastUpdateTime(
                Instant.now().getEpochSecond() - TimeUnit.MILLISECONDS.toSeconds(5 * TEST_REPORTER_FREQ_MILLIS));

        assertThrows(
                IllegalArgumentException.class,
                () -> workerMetricsDAO.updateMetrics(workerMetrics),
                "IllegalArgumentException not thrown on very old LastUpdateTime field value.");
    }

    @Test
    void updateStats_withoutNullRequiredFields_throwIllegalArgumentException() {
        setUp();
        final WorkerMetricStats workerMetrics1 = createDummyWorkerMetrics(TEST_WORKER_ID);
        workerMetrics1.setLastUpdateTime(null);

        assertThrows(
                IllegalArgumentException.class,
                () -> workerMetricsDAO.updateMetrics(workerMetrics1),
                "IllegalArgumentException not thrown on null lastUpdateTime field.");

        final WorkerMetricStats workerMetrics2 = createDummyWorkerMetrics(TEST_WORKER_ID);
        workerMetrics2.setMetricStats(null);
        assertThrows(
                IllegalArgumentException.class,
                () -> workerMetricsDAO.updateMetrics(workerMetrics1),
                "IllegalArgumentException not thrown on null resourcesStats field.");
    }

    @Test
    void getAllWorkerMetrics_sanity() {
        setUp();
        populateNWorkerMetrics(10);

        final List<WorkerMetricStats> response = workerMetricsDAO.getAllWorkerMetricStats();
        assertEquals(10, response.size(), "Invalid no. of workerMetrics item count.");
    }

    @Test
    void deleteStats_sanity() {
        setUp();
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics(TEST_WORKER_ID));

        assertTrue(
                workerMetricsDAO.deleteMetrics(createDummyWorkerMetrics(TEST_WORKER_ID)),
                "DeleteStats operation failed");

        assertEquals(
                0,
                workerMetricsDAO.getAllWorkerMetricStats().size(),
                "WorkerMetricStatsDAO delete did not delete the entry");
    }

    @Test
    void deleteStats_differentLastUpdateTime_asserConditionalFailure() {
        setUp();
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics(TEST_WORKER_ID));

        final WorkerMetricStats workerMetrics = createDummyWorkerMetrics(TEST_WORKER_ID);
        workerMetrics.setLastUpdateTime(0L);

        assertFalse(
                workerMetricsDAO.deleteMetrics(workerMetrics),
                "DeleteStats operation did not failed even with different LastUpdateTime");

        assertEquals(
                1,
                workerMetricsDAO.getAllWorkerMetricStats().size(),
                "WorkerMetricStatsDAO deleteStats conditional check did not work.");
    }

    @Test
    void createProvisionedTable() {
        final WorkerMetricsTableConfig tableConfig = new WorkerMetricsTableConfig(null);
        tableConfig
                .tableName(TEST_WORKER_METRICS_TABLE)
                .billingMode(PROVISIONED)
                .readCapacity(100L)
                .writeCapacity(20L);
        final WorkerMetricStatsDAO workerMetricsDAO =
                new WorkerMetricStatsDAO(dynamoDbAsyncClient, tableConfig, 10000L);
        assertDoesNotThrow(() -> workerMetricsDAO.initialize());
        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_WORKER_METRICS_TABLE)
                        .build())
                .join();
        Assertions.assertEquals(20L, response.table().provisionedThroughput().writeCapacityUnits());
        Assertions.assertEquals(100L, response.table().provisionedThroughput().readCapacityUnits());
    }

    @Test
    void getAllWorkerMetrics_withWorkerMetricsEntryMissingFields_assertGetCallSucceeds() {
        setUp();
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics(TEST_WORKER_ID));
        createAndPutWorkerMetricsEntryAnyRandomAdditionalFieldInTable("SomeWorker2");
        final List<WorkerMetricStats> response = workerMetricsDAO.getAllWorkerMetricStats();
        assertEquals(2, response.size(), "Invalid no. of workerMetrics item count.");
    }

    private WorkerMetricStats getWorkerMetricFromTable(final String workerId) {
        return unwrappingFuture(() -> workerMetricsTable.getItem(
                Key.builder().partitionValue(workerId).build()));
    }

    private void populateNWorkerMetrics(final int n) {
        IntStream.range(0, n)
                .forEach(i -> workerMetricsDAO.updateMetrics(createDummyWorkerMetrics(TEST_WORKER_ID + i)));
    }

    private WorkerMetricStats createDummyWorkerMetrics(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(10D, 12D)))
                .build();
    }

    // This entry is bad as it does not have required field and have some other random field
    private void createAndPutWorkerMetricsEntryAnyRandomAdditionalFieldInTable(final String workerId) {
        final PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(TEST_WORKER_METRICS_TABLE)
                .item(ImmutableMap.of(
                        "wid", AttributeValue.builder().s(workerId).build(),
                        "invalidField", AttributeValue.builder().s("someValue").build()))
                .build();

        dynamoDbAsyncClient.putItem(putItemRequest).join();
    }
}
