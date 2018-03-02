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

import akka.Done
import akka.actor.{ActorSystem, Scheduler}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.pattern.after
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.akka.streams.kafka.AtLeastOnceExample.system
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object HeavyLifting {

  def doSomeHeavyLifting(msg: CommittableMessage[Array[Byte], String])(implicit scheduler: Scheduler, ec: ExecutionContext): Future[CommittableMessage[Array[Byte], String]] = {
    system.log.info(s"~~~> OrderProcessor starts processing message: ${msg.record.value}")

    after(1.second, scheduler){
      system.log.info(s"~~~> OrderProcessor ended processing message: ${msg.record.value.toString}")
      Future.successful(msg)
    }
  }
}

object AtLeastOnceExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {

    implicit val m = ActorMaterializer.create(system)
    implicit val s: Scheduler = system.scheduler

    implicit val ec = system.dispatcher

        system.log.info("~~~> Started application")

        val done =
          Consumer.committableSource(consumerSettings, Subscriptions.topics("grouptopic"))
            .mapAsync(4) { msg =>
              HeavyLifting.doSomeHeavyLifting(msg)
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

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}