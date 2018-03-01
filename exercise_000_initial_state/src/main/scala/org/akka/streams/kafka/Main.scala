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

import java.util.concurrent.atomic.AtomicLong

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object AtLeastOnceExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {

    implicit val m = ActorMaterializer.create(system)
    val db = new DB

    implicit val ec = system.dispatcher

        system.log.info("~~~> Started application")

        val done =
          Consumer.committableSource(consumerSettings, Subscriptions.topics("grouptopic"))
            .mapAsync(1) { msg =>
              system.log.info(s"~~~> Received message: ${msg.record.value()} with offset = ${msg.committableOffset.partitionOffset.offset}")
              db.update(msg.record.value).map(_ => msg)
            }
            .mapAsync(1) { msg =>
              msg.committableOffset.commitScaladsl()
            }
            .runWith(Sink.ignore)

        terminateWhenDone(done)

    terminateWhenDone(done)
  }
}

trait ConsumerExample {
  val system = ActorSystem("example")
  implicit val ec = system.dispatcher

  // #settings
  val consumerSettings: ConsumerSettings[Array[Byte], String] =
    ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("192.168.0.9:32780,192.168.0.9:32779,192.168.0.9:32778")
      .withGroupId("cg1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def business[T]: Flow[T, T, NotUsed] = Flow[T]

  // #db
  class DB {

    private val offset = new AtomicLong

    def save(record: ConsumerRecord[Array[Byte], String]): Future[Done] = {
      system.log.info(s"~~~> DB.save: ${record.value} - offset = $offset")
      offset.set(record.offset)
      Future.successful(Done)
    }

    def loadOffset(): Future[Long] =
      Future.successful(offset.get)

    def update(data: String): Future[Done] = {
      system.log.info(s"~~~> DB.update: $data - offset = $offset")
      Future.successful(Done)
    }
  }
  // #db

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}