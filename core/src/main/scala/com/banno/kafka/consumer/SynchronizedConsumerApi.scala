package com.banno.kafka.consumer

import cats._
import cats.arrow._
import cats.effect._
import cats.effect.concurrent._
import cats.implicits._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization._
import scala.collection.JavaConverters._

/** Instances of [[ConsumerApi]] which have access to most methods synchronized.
  *
  * See
  * [[https://kafka.apache.org/24/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#multithreaded]]
  * for information on which methods are synchronized.
  */
object SynchronizedConsumerApi {

  def create[F[_]: Concurrent, K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    Resource.make[F, ConsumerApi[F, K, V]](
      Semaphore[F](1L).map((s: Semaphore[F]) =>
        new ConsumerApiK[F, F, K, V] {
          override final protected val nat: F ~> F =
            new FunctionK[F, F] {
              override final def apply[A](fa: F[A]): F[A] =
                s.withPermit(fa)
            }

          override final protected val backingConsumer: ConsumerApi[F, K, V] =
            ConsumerImpl.create(
              new KafkaConsumer[K, V](configs.toMap.asJava, keyDeserializer, valueDeserializer)
            )

          // According to the Kafka documentation, the wakeup call is safe to
          // call concurrently.
          override final lazy val wakeup: F[Unit] =
            this.backingConsumer.wakeup
        }
      )
    )(_.close)
}
