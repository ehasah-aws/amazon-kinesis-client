/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.multilang.config;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.converters.ArrayConverter;
import org.apache.commons.beanutils.converters.StringConverter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.ShardPrioritization;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.multilang.config.converter.DurationConverter;
import software.amazon.kinesis.multilang.config.converter.TagConverter;
import software.amazon.kinesis.multilang.config.converter.TagConverter.TagCollection;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

@Getter
@Setter
@Slf4j
public class MultiLangDaemonConfiguration {

    private static final String CREDENTIALS_DEFAULT_SEARCH_PATH = "software.amazon.awssdk.auth.credentials";

    private String applicationName;

    private String streamName;
    private String streamArn;

    @ConfigurationSettable(configurationClass = ConfigsBuilder.class)
    private String tableName;

    private String workerIdentifier = UUID.randomUUID().toString();

    public void setWorkerId(String workerId) {
        this.workerIdentifier = workerId;
    }

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private long failoverTimeMillis;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private long leaseAssignmentIntervalMillis;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private Boolean enablePriorityLeaseAssignment;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private Boolean leaseTableDeletionProtectionEnabled;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private Boolean leaseTablePitrEnabled;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private long shardSyncIntervalMillis;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private boolean cleanupLeasesUponShardCompletion;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private boolean ignoreUnexpectedChildShards;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private int maxLeasesForWorker;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private int maxLeasesToStealAtOneTime;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private int initialLeaseTableReadCapacity;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private int initialLeaseTableWriteCapacity;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class, methodName = "initialPositionInStream")
    @ConfigurationSettable(configurationClass = RetrievalConfig.class)
    private InitialPositionInStreamExtended initialPositionInStreamExtended;

    public InitialPositionInStream getInitialPositionInStream() {
        if (initialPositionInStreamExtended != null) {
            return initialPositionInStreamExtended.getInitialPositionInStream();
        }
        return null;
    }

    public void setInitialPositionInStream(InitialPositionInStream initialPositionInStream) {
        this.initialPositionInStreamExtended =
                InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream);
    }

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private int maxLeaseRenewalThreads;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private long listShardsBackoffTimeInMillis;

    @ConfigurationSettable(configurationClass = LeaseManagementConfig.class)
    private int maxListShardsRetryAttempts;

    // Enables applications flush/checkpoint (if they have some data "in progress", but don't get new data for while)
    @ConfigurationSettable(configurationClass = ProcessorConfig.class)
    private boolean callProcessRecordsEvenForEmptyRecordList;

    @ConfigurationSettable(configurationClass = CoordinatorConfig.class)
    private long parentShardPollIntervalMillis;

    @ConfigurationSettable(configurationClass = CoordinatorConfig.class)
    private ShardPrioritization shardPrioritization;

    @ConfigurationSettable(configurationClass = CoordinatorConfig.class)
    private boolean skipShardSyncAtWorkerInitializationIfLeasesExist;

    @ConfigurationSettable(configurationClass = CoordinatorConfig.class)
    private long schedulerInitializationBackoffTimeMillis;

    @ConfigurationSettable(configurationClass = CoordinatorConfig.class)
    private CoordinatorConfig.ClientVersionConfig clientVersionConfig;

    @ConfigurationSettable(configurationClass = LifecycleConfig.class)
    private long taskBackoffTimeMillis;

    @ConfigurationSettable(configurationClass = MetricsConfig.class)
    private long metricsBufferTimeMillis;

    @ConfigurationSettable(configurationClass = MetricsConfig.class)
    private int metricsMaxQueueSize;

    @ConfigurationSettable(configurationClass = MetricsConfig.class)
    private MetricsLevel metricsLevel;

    @ConfigurationSettable(configurationClass = LifecycleConfig.class, convertToOptional = true)
    private Long logWarningForTaskAfterMillis;

    @ConfigurationSettable(configurationClass = MetricsConfig.class)
    private Set<String> metricsEnabledDimensions;

    public String[] getMetricsEnabledDimensions() {
        return metricsEnabledDimensions.toArray(new String[0]);
    }

    public void setMetricsEnabledDimensions(String[] dimensions) {
        metricsEnabledDimensions = new HashSet<>(Arrays.asList(dimensions));
    }

    private RetrievalMode retrievalMode = RetrievalMode.DEFAULT;

    private final FanoutConfigBean fanoutConfig = new FanoutConfigBean();

    @Delegate(types = PollingConfigBean.PollingConfigBeanDelegate.class)
    private final PollingConfigBean pollingConfig = new PollingConfigBean();

    @Delegate(types = GracefulLeaseHandoffConfigBean.GracefulLeaseHandoffConfigBeanDelegate.class)
    private final GracefulLeaseHandoffConfigBean gracefulLeaseHandoffConfigBean = new GracefulLeaseHandoffConfigBean();

    @Delegate(
            types = WorkerUtilizationAwareAssignmentConfigBean.WorkerUtilizationAwareAssignmentConfigBeanDelegate.class)
    private final WorkerUtilizationAwareAssignmentConfigBean workerUtilizationAwareAssignmentConfigBean =
            new WorkerUtilizationAwareAssignmentConfigBean();

    @Delegate(types = WorkerMetricStatsTableConfigBean.WorkerMetricsTableConfigBeanDelegate.class)
    private final WorkerMetricStatsTableConfigBean workerMetricStatsTableConfigBean =
            new WorkerMetricStatsTableConfigBean();

    @Delegate(types = CoordinatorStateTableConfigBean.CoordinatorStateConfigBeanDelegate.class)
    private final CoordinatorStateTableConfigBean coordinatorStateTableConfigBean =
            new CoordinatorStateTableConfigBean();

    private boolean validateSequenceNumberBeforeCheckpointing;

    private long shutdownGraceMillis;
    private Integer timeoutInSeconds;

    private final BuilderDynaBean kinesisCredentialsProvider;

    public void setAwsCredentialsProvider(String providerString) {
        kinesisCredentialsProvider.set("", providerString);
    }

    private final BuilderDynaBean dynamoDBCredentialsProvider;

    public void setAwsCredentialsProviderDynamoDB(String providerString) {
        dynamoDBCredentialsProvider.set("", providerString);
    }

    private final BuilderDynaBean cloudWatchCredentialsProvider;

    public void setAwsCredentialsProviderCloudWatch(String providerString) {
        cloudWatchCredentialsProvider.set("", providerString);
    }

    private final BuilderDynaBean kinesisClient;
    private final BuilderDynaBean dynamoDbClient;
    private final BuilderDynaBean cloudWatchClient;

    private final BeanUtilsBean utilsBean;
    private final ConvertUtilsBean convertUtilsBean;

    public MultiLangDaemonConfiguration(BeanUtilsBean utilsBean, ConvertUtilsBean convertUtilsBean) {
        this.utilsBean = utilsBean;
        this.convertUtilsBean = convertUtilsBean;

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(Class<T> type, Object value) {
                        Date date = new Date(Long.parseLong(value.toString()) * 1000L);
                        return type.cast(InitialPositionInStreamExtended.newInitialPositionAtTimestamp(date));
                    }
                },
                InitialPositionInStreamExtended.class);

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(Class<T> type, Object value) {
                        return type.cast(MetricsLevel.valueOf(value.toString().toUpperCase()));
                    }
                },
                MetricsLevel.class);

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(Class<T> type, Object value) {
                        return type.cast(
                                InitialPositionInStream.valueOf(value.toString().toUpperCase()));
                    }
                },
                InitialPositionInStream.class);

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(Class<T> type, Object value) {
                        return type.cast(CoordinatorConfig.ClientVersionConfig.valueOf(
                                value.toString().toUpperCase()));
                    }
                },
                CoordinatorConfig.ClientVersionConfig.class);

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(Class<T> type, Object value) {
                        return type.cast(BillingMode.valueOf(value.toString().toUpperCase()));
                    }
                },
                BillingMode.class);

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(Class<T> type, Object value) {
                        return type.cast(URI.create(value.toString()));
                    }
                },
                URI.class);

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(Class<T> type, Object value) {
                        return type.cast(RetrievalMode.from(value.toString()));
                    }
                },
                RetrievalMode.class);

        convertUtilsBean.register(
                new Converter() {
                    @Override
                    public <T> T convert(final Class<T> type, final Object value) {
                        return type.cast(Region.of(value.toString()));
                    }
                },
                Region.class);

        convertUtilsBean.register(new DurationConverter(), Duration.class);
        convertUtilsBean.register(new TagConverter(), TagCollection.class);

        ArrayConverter arrayConverter = new ArrayConverter(String[].class, new StringConverter());
        arrayConverter.setDelimiter(',');
        convertUtilsBean.register(arrayConverter, String[].class);
        AwsCredentialsProviderPropertyValueDecoder credentialsDecoder =
                new AwsCredentialsProviderPropertyValueDecoder();
        Function<String, ?> converter = credentialsDecoder::decodeValue;

        this.kinesisCredentialsProvider = new BuilderDynaBean(
                AwsCredentialsProvider.class, convertUtilsBean, converter, CREDENTIALS_DEFAULT_SEARCH_PATH);
        this.dynamoDBCredentialsProvider = new BuilderDynaBean(
                AwsCredentialsProvider.class, convertUtilsBean, converter, CREDENTIALS_DEFAULT_SEARCH_PATH);
        this.cloudWatchCredentialsProvider = new BuilderDynaBean(
                AwsCredentialsProvider.class, convertUtilsBean, converter, CREDENTIALS_DEFAULT_SEARCH_PATH);

        this.kinesisClient = new BuilderDynaBean(KinesisAsyncClient.class, convertUtilsBean);
        this.dynamoDbClient = new BuilderDynaBean(DynamoDbAsyncClient.class, convertUtilsBean);
        this.cloudWatchClient = new BuilderDynaBean(CloudWatchAsyncClient.class, convertUtilsBean);
    }

    private void setRegionForClient(String name, BuilderDynaBean client, Region region) {
        try {
            utilsBean.setProperty(client, "region", region);
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.error("Failed to set region on {}", name, e);
            throw new IllegalStateException(e);
        }
    }

    public void setRegionName(Region region) {
        setRegionForClient("kinesisClient", kinesisClient, region);
        setRegionForClient("dynamoDbClient", dynamoDbClient, region);
        setRegionForClient("cloudWatchClient", cloudWatchClient, region);
    }

    private void setEndpointForClient(String name, BuilderDynaBean client, String endpoint) {
        try {
            utilsBean.setProperty(client, "endpointOverride", endpoint);
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.error("Failed to set endpoint on {}", name, e);
            throw new IllegalStateException(e);
        }
    }

    public void setKinesisEndpoint(String endpoint) {
        setEndpointForClient("kinesisClient", kinesisClient, endpoint);
    }

    public void setDynamoDBEndpoint(String endpoint) {
        setEndpointForClient("dynamoDbClient", dynamoDbClient, endpoint);
    }

    private AwsCredentialsProvider resolveCredentials(BuilderDynaBean credsBuilder) {
        if (!credsBuilder.isDirty()) {
            return null;
        }
        return credsBuilder.build(AwsCredentialsProvider.class);
    }

    private void updateCredentials(
            BuilderDynaBean toUpdate, AwsCredentialsProvider primary, AwsCredentialsProvider secondary) {

        if (toUpdate.hasValue("credentialsProvider")) {
            return;
        }

        try {
            if (primary != null) {
                utilsBean.setProperty(toUpdate, "credentialsProvider", primary);
            } else if (secondary != null) {
                utilsBean.setProperty(toUpdate, "credentialsProvider", secondary);
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to update credentials", e);
        }
    }

    private void addConfigObjects(Map<Class<?>, Object> configObjects, Object... toAdd) {
        for (Object obj : toAdd) {
            configObjects.put(obj.getClass(), obj);
        }
    }

    private void resolveFields(Map<Class<?>, Object> configObjects, Set<Class<?>> restrictTo, Set<Class<?>> skipIf) {
        ConfigurationSettableUtils.resolveFields(this, configObjects, restrictTo, skipIf);
    }

    private void handleRetrievalConfig(RetrievalConfig retrievalConfig, ConfigsBuilder configsBuilder) {
        retrievalConfig.retrievalSpecificConfig(
                retrievalMode.builder(this).build(configsBuilder.kinesisClient(), this));
    }

    private void handleCoordinatorConfig(CoordinatorConfig coordinatorConfig) {
        ConfigurationSettableUtils.resolveFields(
                this.coordinatorStateTableConfigBean, coordinatorConfig.coordinatorStateTableConfig());
    }

    private void handleLeaseManagementConfig(LeaseManagementConfig leaseManagementConfig) {
        ConfigurationSettableUtils.resolveFields(
                this.gracefulLeaseHandoffConfigBean, leaseManagementConfig.gracefulLeaseHandoffConfig());
        ConfigurationSettableUtils.resolveFields(
                this.workerUtilizationAwareAssignmentConfigBean,
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig());
        ConfigurationSettableUtils.resolveFields(
                this.workerMetricStatsTableConfigBean,
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig().workerMetricsTableConfig());
    }

    private Object adjustKinesisHttpConfiguration(Object builderObj) {
        if (builderObj instanceof KinesisAsyncClientBuilder) {
            KinesisAsyncClientBuilder builder = (KinesisAsyncClientBuilder) builderObj;
            return builder.applyMutation(KinesisClientUtil::adjustKinesisClientBuilder);
        }

        return builderObj;
    }

    @Data
    static class ResolvedConfiguration {
        final CoordinatorConfig coordinatorConfig;
        final CheckpointConfig checkpointConfig;
        final LeaseManagementConfig leaseManagementConfig;
        final LifecycleConfig lifecycleConfig;
        final MetricsConfig metricsConfig;
        final ProcessorConfig processorConfig;
        final RetrievalConfig retrievalConfig;

        public Scheduler build() {
            return new Scheduler(
                    checkpointConfig,
                    coordinatorConfig,
                    leaseManagementConfig,
                    lifecycleConfig,
                    metricsConfig,
                    processorConfig,
                    retrievalConfig);
        }
    }

    ResolvedConfiguration resolvedConfiguration(ShardRecordProcessorFactory shardRecordProcessorFactory) {
        AwsCredentialsProvider kinesisCreds = resolveCredentials(kinesisCredentialsProvider);
        AwsCredentialsProvider dynamoDbCreds = resolveCredentials(dynamoDBCredentialsProvider);
        AwsCredentialsProvider cloudwatchCreds = resolveCredentials(cloudWatchCredentialsProvider);

        updateCredentials(kinesisClient, kinesisCreds, kinesisCreds);
        updateCredentials(dynamoDbClient, dynamoDbCreds, kinesisCreds);
        updateCredentials(cloudWatchClient, cloudwatchCreds, kinesisCreds);

        KinesisAsyncClient kinesisAsyncClient =
                kinesisClient.build(KinesisAsyncClient.class, this::adjustKinesisHttpConfiguration);
        DynamoDbAsyncClient dynamoDbAsyncClient = dynamoDbClient.build(DynamoDbAsyncClient.class);
        CloudWatchAsyncClient cloudWatchAsyncClient = cloudWatchClient.build(CloudWatchAsyncClient.class);

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                streamName,
                applicationName,
                kinesisAsyncClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                workerIdentifier,
                shardRecordProcessorFactory);

        Map<Class<?>, Object> configObjects = new HashMap<>();
        addConfigObjects(configObjects, configsBuilder);

        resolveFields(
                configObjects, Collections.singleton(ConfigsBuilder.class), Collections.singleton(PollingConfig.class));

        CoordinatorConfig coordinatorConfig = configsBuilder.coordinatorConfig();
        CheckpointConfig checkpointConfig = configsBuilder.checkpointConfig();
        LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig();
        LifecycleConfig lifecycleConfig = configsBuilder.lifecycleConfig();
        MetricsConfig metricsConfig = configsBuilder.metricsConfig();
        ProcessorConfig processorConfig = configsBuilder.processorConfig();
        RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig();

        addConfigObjects(
                configObjects,
                coordinatorConfig,
                checkpointConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);

        handleCoordinatorConfig(coordinatorConfig);
        handleLeaseManagementConfig(leaseManagementConfig);
        handleRetrievalConfig(retrievalConfig, configsBuilder);

        resolveFields(configObjects, null, new HashSet<>(Arrays.asList(ConfigsBuilder.class, PollingConfig.class)));

        return new ResolvedConfiguration(
                coordinatorConfig,
                checkpointConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
    }

    public Scheduler build(ShardRecordProcessorFactory shardRecordProcessorFactory) {
        return resolvedConfiguration(shardRecordProcessorFactory).build();
    }
}
