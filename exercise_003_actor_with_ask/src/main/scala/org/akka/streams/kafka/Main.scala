/**
  * Copyright © 2018 Lightbend, Inc
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * NO COMMERCIAL SUPPORT OR ANY OTHER FORM OF SUPPORT IS OFFERED ON
  * THIS SOFTWARE BY LIGHTBEND, Inc.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package org.akka.streams.kafka

import akka.actor.{ActorSystem, Props}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset, CommittableOffsetBatch}
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.routing.FromConfig
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._

object AtLeastOnceExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {

    implicit val m = ActorMaterializer.create(system)

    implicit val ec = system.dispatcher

    implicit val askTimeout: Timeout = 10.seconds

    system.log.info("~~~> Started application")

    val orderProcessor = system.actorOf(FromConfig.props(OrderProcessor.props), "order-processor")

    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("grouptopic"))
        .ask[CommittableMessage[ConsumerRecord[Array[Byte], String], CommittableOffset]](parallelism = 4)(orderProcessor)
        .groupedWithin(10, 5.seconds)
        .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem.committableOffset) })
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)

    system.log.info("~~~> Scheduled work...")

  }
}

trait ConsumerExample {
  val system = ActorSystem("example")
  implicit val ec = system.dispatcher

  private val config = system.settings.config

  private val bootstrapServers = config.getString("akka-streams-kafka.bootstrap-servers")

  system.log.info(s"Starting with bootstrap servers: $bootstrapServers")

  // #settings
  val consumerSettings: ConsumerSettings[Array[Byte], String] =
    ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("192.168.0.55:32771,192.168.0.55:32770,192.168.0.55:32769")
      .withGroupId("cg1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

}