/*
 * Copyright 2019 Jack Henry & Associates, Inc.®
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.banno.kafka.consumer

import cats.implicits._
import cats.effect.{Async, ContextShift, Resource, Sync}
import fs2.Stream
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.clients.consumer._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.FromRecord
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import com.banno.kafka._
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

trait ConsumerApi[F[_], K, V] {
  def assign(partitions: Iterable[TopicPartition]): F[Unit]
  def assignment: F[Set[TopicPartition]]
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]]
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]
  def close: F[Unit]
  def close(timeout: FiniteDuration): F[Unit]
  def commitAsync: F[Unit]
  def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit]
  def commitAsync(callback: OffsetCommitCallback): F[Unit]
  def commitSync: F[Unit]
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
  def committed(partition: TopicPartition): F[OffsetAndMetadata]
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]]
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]
  def listTopics: F[Map[String, Seq[PartitionInfo]]]
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]]
  def metrics: F[Map[MetricName, Metric]]
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]]
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndTimestamp]]
  def partitionsFor(topic: String): F[Seq[PartitionInfo]]
  def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]]
  def pause(partitions: Iterable[TopicPartition]): F[Unit]
  def paused: F[Set[TopicPartition]]
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]
  def position(partition: TopicPartition): F[Long]
  def resume(partitions: Iterable[TopicPartition]): F[Unit]
  def seek(partition: TopicPartition, offset: Long): F[Unit]
  def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit]
  def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit]
  def subscribe(topics: Iterable[String]): F[Unit]
  def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit]
  def subscribe(pattern: Pattern): F[Unit]
  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit]
  def subscription: F[Set[String]]
  def unsubscribe: F[Unit]
  def wakeup: F[Unit]
}

object ConsumerApi {

  def createKafkaConsumer[F[_]: Sync, K, V](configs: (String, AnyRef)*): F[KafkaConsumer[K, V]] =
    Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava))

  def createKafkaConsumer[F[_]: Sync, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): F[KafkaConsumer[K, V]] =
    Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava, keyDeserializer, valueDeserializer))

  object BlockingContext {

    //TODO ThreadFactory to name thread properly
    def default[F[_]: Sync]: F[ExecutionContextExecutorService] =
      Sync[F].delay(ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor()))

    def resource[F[_]: Sync]: Resource[F, ExecutionContextExecutorService] =
      Resource.make(default[F])(a => Sync[F].delay(a.shutdown()))
  }

  def apply[F[_]: Async: ContextShift, K, V](
      configs: (String, AnyRef)*
  ): F[(ConsumerApi[F, K, V], ExecutionContextExecutorService)] =
    for {
      c <- createKafkaConsumer[F, K, V](configs: _*).map(ConsumerImpl(_))
      e <- BlockingContext.default
    } yield (ShiftingConsumerImpl(c, e), e)

  def apply[F[_]: Async: ContextShift, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): F[(ConsumerApi[F, K, V], ExecutionContextExecutorService)] =
    for {
      c <- createKafkaConsumer[F, K, V](keyDeserializer, valueDeserializer, configs: _*)
        .map(ConsumerImpl(_))
      e <- BlockingContext.default
    } yield (ShiftingConsumerImpl(c, e), e)

  def create[F[_]: Async: ContextShift, K: Deserializer, V: Deserializer](
      configs: (String, AnyRef)*
  ): F[(ConsumerApi[F, K, V], ExecutionContextExecutorService)] =
    apply[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

  def resource[F[_]: Async: ContextShift, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    BlockingContext.resource.flatMap(
      e =>
        Resource.make(
          createKafkaConsumer[F, K, V](keyDeserializer, valueDeserializer, configs: _*)
            .map(c => ShiftingConsumerImpl.create(ConsumerImpl(c), e))
        )(_.close)
    )

  def resource[F[_]: Async: ContextShift, K: Deserializer, V: Deserializer](
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    resource[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

  def stream[F[_]: Async: ContextShift, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): Stream[F, ConsumerApi[F, K, V]] =
    Stream.resource(resource[F, K, V](keyDeserializer, valueDeserializer, configs: _*))

  def stream[F[_]: Async: ContextShift, K: Deserializer, V: Deserializer](
      configs: (String, AnyRef)*
  ): Stream[F, ConsumerApi[F, K, V]] =
    stream[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

  object Avro {

    def create[F[_]: Async: ContextShift, K, V](
        configs: (String, AnyRef)*
    ): F[(ConsumerApi[F, K, V], ExecutionContextExecutorService)] =
      ConsumerApi[F, K, V](
        (
          configs.toMap +
            KeyDeserializerClass(classOf[KafkaAvroDeserializer]) +
            ValueDeserializerClass(classOf[KafkaAvroDeserializer])
        ).toSeq: _*
      )

    def resource[F[_]: Async: ContextShift, K, V](
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      BlockingContext.resource.flatMap(
        e =>
          Resource.make(
            createKafkaConsumer[F, K, V](
              (
                configs.toMap +
                  KeyDeserializerClass(classOf[KafkaAvroDeserializer]) +
                  ValueDeserializerClass(classOf[KafkaAvroDeserializer])
              ).toSeq: _*
            ).map(c => ShiftingConsumerImpl.create(ConsumerImpl(c), e))
          )(_.close)
      )

    def stream[F[_]: Async: ContextShift, K, V](
        configs: (String, AnyRef)*
    ): Stream[F, ConsumerApi[F, K, V]] =
      Stream.resource(resource[F, K, V](configs: _*))

    object Generic {

      def create[F[_]: Async: ContextShift](
          configs: (String, AnyRef)*
      ): F[(ConsumerApi[F, GenericRecord, GenericRecord], ExecutionContextExecutorService)] =
        ConsumerApi.Avro.create[F, GenericRecord, GenericRecord](configs: _*)

      def resource[F[_]: Async: ContextShift](
          configs: (String, AnyRef)*
      ): Resource[F, ConsumerApi[F, GenericRecord, GenericRecord]] =
        ConsumerApi.Avro.resource[F, GenericRecord, GenericRecord](configs: _*)

      def stream[F[_]: Async: ContextShift](
          configs: (String, AnyRef)*
      ): Stream[F, ConsumerApi[F, GenericRecord, GenericRecord]] =
        Stream.resource(resource[F](configs: _*))
    }

    object Specific {

      def create[F[_]: Async: ContextShift, K, V](
          configs: (String, AnyRef)*
      ): F[(ConsumerApi[F, K, V], ExecutionContextExecutorService)] =
        ConsumerApi.Avro.create[F, K, V]((configs.toMap + SpecificAvroReader(true)).toSeq: _*)

      def resource[F[_]: Async: ContextShift, K, V](
          configs: (String, AnyRef)*
      ): Resource[F, ConsumerApi[F, K, V]] =
        ConsumerApi.Avro.resource[F, K, V]((configs.toMap + SpecificAvroReader(true)).toSeq: _*)

      def stream[F[_]: Async: ContextShift, K, V](
          configs: (String, AnyRef)*
      ): Stream[F, ConsumerApi[F, K, V]] =
        Stream.resource(resource[F, K, V](configs: _*))
    }
  }

  object Avro4s {

    def create[F[_]: Async: ContextShift, K: FromRecord, V: FromRecord](
        configs: (String, AnyRef)*
    ): F[(ConsumerApi[F, K, V], ExecutionContextExecutorService)] =
      ConsumerApi.Avro.Generic.create[F](configs: _*).map {
        case (c, e) => (Avro4sConsumerImpl(c), e)
      }

    def resource[F[_]: Async: ContextShift, K: FromRecord, V: FromRecord](
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      ConsumerApi.Avro.Generic.resource[F](configs: _*).map(Avro4sConsumerImpl(_))

    def stream[F[_]: Async: ContextShift, K: FromRecord, V: FromRecord](
        configs: (String, AnyRef)*
    ): Stream[F, ConsumerApi[F, K, V]] =
      Stream.resource(resource[F, K, V](configs: _*))
  }

  object NonShifting {

    def apply[F[_]: Sync, K, V](
        configs: (String, AnyRef)*
    ): F[ConsumerApi[F, K, V]] =
      createKafkaConsumer[F, K, V](configs: _*).map(ConsumerImpl(_))

    def apply[F[_]: Sync, K, V](
        keyDeserializer: Deserializer[K],
        valueDeserializer: Deserializer[V],
        configs: (String, AnyRef)*
    ): F[ConsumerApi[F, K, V]] =
      createKafkaConsumer[F, K, V](keyDeserializer, valueDeserializer, configs: _*)
        .map(ConsumerImpl(_))

    def create[F[_]: Sync, K: Deserializer, V: Deserializer](
        configs: (String, AnyRef)*
    ): F[ConsumerApi[F, K, V]] =
      apply[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

    def resource[F[_]: Sync, K, V](
        keyDeserializer: Deserializer[K],
        valueDeserializer: Deserializer[V],
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      Resource.make(apply[F, K, V](keyDeserializer, valueDeserializer, configs: _*))(_.close)

    def resource[F[_]: Sync, K: Deserializer, V: Deserializer](
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      resource[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

    def stream[F[_]: Sync, K, V](
        keyDeserializer: Deserializer[K],
        valueDeserializer: Deserializer[V],
        configs: (String, AnyRef)*
    ): Stream[F, ConsumerApi[F, K, V]] =
      Stream.resource(resource[F, K, V](keyDeserializer, valueDeserializer, configs: _*))

    def stream[F[_]: Sync, K: Deserializer, V: Deserializer](
        configs: (String, AnyRef)*
    ): Stream[F, ConsumerApi[F, K, V]] =
      stream[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)
  }
}
