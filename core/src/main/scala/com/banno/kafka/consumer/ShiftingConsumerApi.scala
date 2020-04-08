package com.banno.kafka.consumer

import cats._
import cats.arrow._
import cats.effect._
import org.apache.kafka.common.serialization._

object ShiftingConsumerApi {
  def apply[F[_]: Concurrent: ContextShift, K, V](
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
    }

  def create[F[_]: Concurrent: ContextShift, K, V](
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
