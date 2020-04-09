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

  def create[F[_], K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    configs: (String, AnyRef)*
  )(implicit F: ConcurrentEffect[F]): Resource[F, ConsumerApi[F, K, V]] =
    Resource.make[F, ConsumerApi[F, K, V]](
      Semaphore[F](1L).map((s: Semaphore[F]) =>
        new ConsumerApiK[F, F, K, V] {
          override final protected lazy val nat: F ~> F =
            new FunctionK[F, F] {
              override final def apply[A](fa: F[A]): F[A] =
                s.withPermit(
                  F.cancelable { cb =>
                    val token: CancelToken[F] = F.runCancelable(fa)(r =>
                      IO(cb(r))
                    ).unsafeRunSync

                    // According to the Kafka documentation, the intended
                    // mechanism for interrupting a call to the Consumer API
                    // is to invoke the wakeup method. Thus, here we invoke
                    // _both_ the wakeup method _and_ the CancelToken return
                    // by runCancelable.
                    backingConsumer.wakeup *> token
                  }
                )
            }

          override final protected lazy val backingConsumer: ConsumerApi[F, K, V] =
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
