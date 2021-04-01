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
package org.apache.kafka.connect.connector;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.components.Versioned;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * Connectors manage integration of Kafka Connect with another system, either as an input that ingests
 * data into Kafka or an output that passes data to an external system. Implementations should
 * not use this class directly; they should inherit from {@link org.apache.kafka.connect.source.SourceConnector SourceConnector}
 * or {@link org.apache.kafka.connect.sink.SinkConnector SinkConnector}.
 * </p>
 * 连接器管理与另一个系统的Kafka连接的集成，
 * 要么作为摄入数据到Kafka的输入，要么作为传递数据到外部系统的输出。实现不应该直接使用这个类;
 * <p>
 * Connectors have two primary tasks. First, given some configuration, they are responsible for
 * creating configurations for a set of {@link Task}s that split up the data processing. For
 * example, a database Connector might create Tasks by dividing the set of tables evenly among
 * tasks. Second, they are responsible for monitoring inputs for changes that require
 * reconfiguration and notifying the Kafka Connect runtime via the {@link ConnectorContext}. Continuing the
 * previous example, the connector might periodically check for new tables and notify Kafka Connect of
 * additions and deletions. Kafka Connect will then request new configurations and update the running
 * Tasks.
 * </p>
 * 连接器有两个主要任务。首先，给定一些配置，他们负责为一组分割数据处理的{@link任务}创建配置。
 * 例如，数据库连接器可以通过在任务之间均匀地划分表集来创建任务。
 * 其次，他们负责监控需要重新配置的更改输入，并通过{@link ConnectorContext}通知Kafka Connect运行时。
 * 继续前面的示例，连接器可能会定期检查新表，并将添加和删除通知Kafka Connect。
 * 然后Kafka Connect将请求新的配置并更新正在运行的任务。
 */
public abstract class Connector implements Versioned {

    protected ConnectorContext context;


    /**
     * Initialize this connector, using the provided ConnectorContext to notify the runtime of
     * input configuration changes.
     * 初始化此连接器，使用提供的ConnectorContext通知运行时输入配置更改。
     * @param ctx context object used to interact with the Kafka Connect runtime
     */
    public void initialize(ConnectorContext ctx) {
        context = ctx;
    }

    /**
     * <p>
     * Initialize this connector, using the provided ConnectorContext to notify the runtime of
     * input configuration changes and using the provided set of Task configurations.
     * This version is only used to recover from failures.
     * 初始化此连接器，使用提供的ConnectorContext通知运行时输入配置更改，并使用提供的任务配置集。
     * 此版本仅用于从失败中恢复。
     * </p>
     * <p>
     * The default implementation ignores the provided Task configurations. During recovery, Kafka Connect will request
     * an updated set of configurations and update the running Tasks appropriately. However, Connectors should
     * implement special handling of this case if it will avoid unnecessary changes to running Tasks.
     * </p>
     * 默认实现忽略提供的任务配置。
     * 在恢复期间，Kafka Connect将请求一组更新的配置，并相应地更新正在运行的任务。
     * 但是，如果连接器能够避免对正在运行的任务进行不必要的更改，则应该实现对这种情况的特殊处理。
     *
     * @param ctx context object used to interact with the Kafka Connect runtime
     * @param taskConfigs existing task configurations, which may be used when generating new task configs to avoid
     *                    churn in partition to task assignments
     */
    public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
        context = ctx;
        // Ignore taskConfigs. May result in more churn of tasks during recovery if updated configs
        // are very different, but reduces the difficulty of implementing a Connector
    }

    /**
     * Returns the context object used to interact with the Kafka Connect runtime.
     * 返回用于与Kafka连接运行时交互的上下文对象。
     *
     * @return the context for this Connector.
     */
    protected ConnectorContext context() {
        return context;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    public abstract void start(Map<String, String> props);

    /**
     * Reconfigure this Connector. Most implementations will not override this, using the default
     * implementation that calls {@link #stop()} followed by {@link #start(Map)}.
     * Implementations only need to override this if they want to handle this process more
     * efficiently, e.g. without shutting down network connections to the external system.
     *
     * @param props new configuration settings
     */
    public void reconfigure(Map<String, String> props) {
        stop();
        start(props);
    }

    /**
     * Returns the Task implementation for this Connector.
     * 返回此连接器的任务实现。
     */
    public abstract Class<? extends Task> taskClass();

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * 根据当前配置为任务返回一组配置，最多生成计数配置。
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    public abstract List<Map<String, String>> taskConfigs(int maxTasks);

    /**
     * Stop this connector.
     */
    public abstract void stop();

    /**
     * Validate the connector configuration values against configuration definitions.
     * 根据配置定义验证连接器配置值。
     * @param connectorConfigs the provided configuration values
     * @return List of Config, each Config contains the updated configuration information given
     * the current configuration values.
     */
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef configDef = config();
        if (null == configDef) {
            throw new ConnectException(
                String.format("%s.config() must return a ConfigDef that is not null.", this.getClass().getName())
            );
        }
        List<ConfigValue> configValues = configDef.validate(connectorConfigs);
        return new Config(configValues);
    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector; may not be null.
     */
    public abstract ConfigDef config();
}
