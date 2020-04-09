package com.banno.kafka.consumer

import cats._
import cats.arrow._
import cats.effect._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._
import org.apache.kafka.common.serialization._

object ShiftingConsumerApi {
  def apply[F[_]: ConcurrentEffect: ContextShift, K, V](
    blocker: Blocker
  )(
    consumerApi: ConsumerApi[F, K, V]
  ): ConsumerApi[F, K, V] =
    new ConsumerApiK[F, F, K, V] {
      override final protected val nat: F ~> F =
        new FunctionK[F, F] {
          override final def apply[A](fa: F[A]): F[A] =
            blocker.blockOn(fa)
        }
      override final protected val backingConsumer: ConsumerApi[F, K, V] =
        consumerApi

      // According to the Kafka documentation, these calls will not
      // block. This makes them suitable for any thread pool.

      override final def commitAsync: F[Unit] =
        this.backingConsumer.commitAsync
      override final def commitAsync(
        offsets: Map[TopicPartition, OffsetAndMetadata],
        callback: OffsetCommitCallback
      ): F[Unit] =
        this.backingConsumer.commitAsync(offsets, callback)
      override final def commitAsync(callback: OffsetCommitCallback): F[Unit] =
        this.backingConsumer.commitAsync(callback)
    }

  def create[F[_]: ConcurrentEffect: ContextShift, K, V](
    blocker: Blocker
  )(
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
      SynchronizedConsumerApi.create[F, K, V](
        keyDeserializer,
        valueDeserializer,
        configs: _*
      ).map(ShiftingConsumerApi.apply(blocker)(_))
}
