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
package kafka.metrics

import com.yammer.metrics.core.Timer

/**
 * A wrapper around metrics timer object that provides a convenient mechanism
 * to time code blocks. This pattern was borrowed from the metrics-scala_2.9.1
 * package.
 * @param metric The underlying timer object.
 *
 *
 * 度量计时器对象的包装器提供了一种方便的机制来计时代码块。这个模式是从metrics-scala_2.9.1包中借用的。
 * 度量底层计时器对象。
 */
class KafkaTimer(metric: Timer) {

  def time[A](f: => A): A = {
    val ctx = metric.time
    try f
    finally ctx.stop()
  }
}

