package org.akka.streams.kafka

import akka.actor.{Actor, ActorRef, ActorLogging, Props, Stash, Timers}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._

object OrderProcessor {

  case object Init
  case object Ack
  case object Done

  case object TimerDone

  def props: Props = Props(new OrderProcessor)
}

class OrderProcessor extends Actor with ActorLogging with Stash with Timers {

  import OrderProcessor._

  override def receive: Receive = idle

  def idle: Receive = {
    case Init =>
      log.info("~~~> OrderProcessor: Received Init")
      sender() ! Ack

    case Ack => log.info("OrderProcessor: received Ack")

    case Done =>
      log.info(s"~~~> OrderProcessor: no more elements - end of stream")

    case msg: CommittableMessage[ConsumerRecord[Array[Byte], String], CommittableOffset] =>
      log.info(s"~~~> OrderProcessor starts processing message: ${msg.record.value}")
      timers.startSingleTimer("process-timer", TimerDone, 1.seconds)
      context.become(busy(msg, sender))

  }

  def busy(msg: CommittableMessage[ConsumerRecord[Array[Byte], String], CommittableOffset], sndr: ActorRef): Receive = {
    case TimerDone =>
      log.info(s"~~~> OrderProcessor ended processing message: ${msg.record.value.toString}")
      context.become(idle)
      sndr ! Ack
      msg.committableOffset.commitScaladsl()
      unstashAll()

    case msg =>
      log.info(s"~~~> OrderProcessor stashing message: ${msg.toString}")
      stash()
  }

}
