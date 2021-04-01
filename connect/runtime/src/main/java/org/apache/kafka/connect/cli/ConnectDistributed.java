/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.cli;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * <p>
 * Command line utility that runs Kafka Connect in distributed mode. In this mode, the process joints a group of other workers
 * and work is distributed among them. This is useful for running Connect as a service, where connectors can be
 * submitted to the cluster to be automatically executed in a scalable, distributed fashion. This also allows you to
 * easily scale out horizontally, elastically adding or removing capacity simply by starting or stopping worker
 * instances.
 * </p>
 */
public class ConnectDistributed {
    private static final Logger log = LoggerFactory.getLogger(ConnectDistributed.class);

    private final Time time = Time.SYSTEM;
    private final long initStart = time.hiResClockMs();

    public static void main(String[] args) {

        if (args.length < 1 || Arrays.asList(args).contains("--help")) {
            log.info("Usage: ConnectDistributed worker.properties");
            Exit.exit(1);
        }

        try {
            WorkerInfo initInfo = new WorkerInfo();
            initInfo.logAll();

            String workerPropsFile = args[0];
            // 配置文件转换为map
            Map<String, String> workerProps = !workerPropsFile.isEmpty() ?
                    Utils.propsToStringMap(Utils.loadProps(workerPropsFile)) : Collections.emptyMap();

            ConnectDistributed connectDistributed = new ConnectDistributed();
            Connect connect = connectDistributed.startConnect(workerProps);

            // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
            connect.awaitStop();

        } catch (Throwable t) {
            log.error("Stopping due to error", t);
            Exit.exit(2);
        }
    }

    public Connect startConnect(Map<String, String> workerProps) {
        log.info("Scanning for plugin classes. This might take a moment ...");
        // 插件注册
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        // 分布式配置加载
        DistributedConfig config = new DistributedConfig(workerProps);

        String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
        log.debug("Kafka cluster ID: {}", kafkaClusterId);

        // 启动 jetty server，构建 rest 服务用于管理kafka connector，
        RestServer rest = new RestServer(config);
        rest.initializeServer();

        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        // 使用一个 topic 来存储 connector 涉及的 offset 信息
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);

        // 具体执行工作
        Worker worker = new Worker(workerId, time, plugins, config, offsetBackingStore, connectorClientConfigOverridePolicy);
        WorkerConfigTransformer configTransformer = worker.configTransformer();

        Converter internalValueConverter = worker.getInternalValueConverter();

        // 使用一个 topic 来存储 connector 与 task 的状态
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter);
        statusBackingStore.configure(config);

        // 使用一个 topic 来存储 connector config 信息
        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                config,
                configTransformer);

        // 分布式 connector
        DistributedHerder herder = new DistributedHerder(config, time, worker,
                kafkaClusterId, statusBackingStore, configBackingStore,
                advertisedUrl.toString(), connectorClientConfigOverridePolicy);

        final Connect connect = new Connect(herder, rest);
        log.info("Kafka Connect distributed worker initialization took {}ms", time.hiResClockMs() - initStart);
        try {
            // 启动 DistributedHerder 与 jetty server,
            // DistributedHerder 将会启动 connector task
            connect.start();
        } catch (Exception e) {
            log.error("Failed to start Connect", e);
            connect.stop();
            Exit.exit(3);
        }

        return connect;
    }

}
