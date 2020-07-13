/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import java.util

import kafka.common.BrokerEndPointNotAvailableException
import kafka.server.KafkaConfig
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.feature.Features._
import org.apache.kafka.common.{ClusterResource, Endpoint, Node}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.authorizer.AuthorizerServerInfo

import scala.collection.Seq
import scala.jdk.CollectionConverters._

object Broker {
  private[cluster] case class ServerInfo(clusterResource: ClusterResource,
                                         brokerId: Int,
                                         endpoints: util.List[Endpoint],
                                         interBrokerEndpoint: Endpoint) extends AuthorizerServerInfo

  def apply(id: Int, endPoints: Seq[EndPoint], rack: Option[String]): Broker = {
    new Broker(id, endPoints, rack, emptySupportedFeatures)
  }
}

/**
 * A Kafka broker.
 *
 * @param id          a broker id
 *                    每一个boker都有一个唯一的id作为它们的名字。 这就允许boker切换到别的主机/端口上， consumer依然知道
 * @param endPoints   a collection of EndPoint. Each end-point is (host, port, listener name, security protocol).
 *                    它就是你定义的 Kafka Broker 连接信息，比如 PLAINTEXT://localhost:9092。
 *                    Acceptor 需要用到 endPoint 包含的主机名和端口信息创建 Server Socket。
 * @param rack        an optional rack 指定broker机架信息。
 *                    若设置了机架信息，kafka在分配副本时会考虑把某个分区的多个副本分配在多个机架上，
 *                    这样即使某个机架上的broker全部崩溃，也能保证其他机架上的副本可以正常工作
 * @param features    supported features
 */
case class Broker(id: Int, endPoints: Seq[EndPoint], rack: Option[String], features: Features[SupportedVersionRange]) {

  private val endPointsMap = endPoints.map { endPoint =>
    endPoint.listenerName -> endPoint
  }.toMap

  if (endPointsMap.size != endPoints.size)
    throw new IllegalArgumentException(s"There is more than one end point with the same listener name: ${endPoints.mkString(",")}")

  override def toString: String =
    s"$id : ${endPointsMap.values.mkString("(",",",")")} : ${rack.orNull} : $features"

  def this(id: Int, host: String, port: Int, listenerName: ListenerName, protocol: SecurityProtocol) = {
    this(id, Seq(EndPoint(host, port, listenerName, protocol)), None, emptySupportedFeatures)
  }

  def this(bep: BrokerEndPoint, listenerName: ListenerName, protocol: SecurityProtocol) = {
    this(bep.id, bep.host, bep.port, listenerName, protocol)
  }

  def node(listenerName: ListenerName): Node =
    getNode(listenerName).getOrElse {
      throw new BrokerEndPointNotAvailableException(s"End point with listener name ${listenerName.value} not found " +
        s"for broker $id")
    }

  def getNode(listenerName: ListenerName): Option[Node] =
    endPointsMap.get(listenerName).map(endpoint => new Node(id, endpoint.host, endpoint.port, rack.orNull))

  def brokerEndPoint(listenerName: ListenerName): BrokerEndPoint = {
    val endpoint = endPoint(listenerName)
    new BrokerEndPoint(id, endpoint.host, endpoint.port)
  }

  def endPoint(listenerName: ListenerName): EndPoint = {
    endPointsMap.getOrElse(listenerName,
      throw new BrokerEndPointNotAvailableException(s"End point with listener name ${listenerName.value} not found for broker $id"))
  }

  def toServerInfo(clusterId: String, config: KafkaConfig): AuthorizerServerInfo = {
    val clusterResource: ClusterResource = new ClusterResource(clusterId)
    val interBrokerEndpoint: Endpoint = endPoint(config.interBrokerListenerName).toJava
    val brokerEndpoints: util.List[Endpoint] = endPoints.toList.map(_.toJava).asJava
    Broker.ServerInfo(clusterResource, id, brokerEndpoints, interBrokerEndpoint)
  }
}
