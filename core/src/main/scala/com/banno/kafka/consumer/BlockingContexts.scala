package com.banno.kafka.consumer

import cats.effect._
import cats.effect.concurrent._
import cats.implicits._
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/** Provides a default `cats.effect.Blocker` for use with a Kafka4s. */
object BlockingContext {

  private[this] val poolCounter: Ref[IO, Long] =
    Ref.unsafe[IO, Long](0L)

  private[this] def newThreadFactory[F[_]](implicit F: LiftIO[F], FS: Sync[F]): F[ThreadFactory] =
    // Because poolCounter is statically initialized global variable, we
    // need to suspend before access in order to not violate RF.
    //
    // https://github.com/typelevel/cats-effect/blob/v2.0.0/core/shared/src/main/scala/cats/effect/concurrent/Ref.scala#L176
    FS.suspend(
      F.liftIO(this.poolCounter.modify(l => (l + 1L, l))).map((poolCounter: Long) =>
        new ThreadFactory {

          // We are firmly in Java land here. So we need to use Java level atomic primitives.
          private val threadCounter: AtomicLong =
            new AtomicLong(0L)
          private val backingFactory: ThreadFactory =
            Executors.defaultThreadFactory()

          override final def newThread(r: Runnable): Thread = {
            val t: Thread = this.backingFactory.newThread(r)
            val id: Long = this.threadCounter.incrementAndGet()
            t.setName(s"kafka4s-pool${poolCounter}-${id}")
            t
          }
        }
      )
    )

  /** A default `cats.effect.Blocker`. */
  def defaultBlocker[F[_]: LiftIO: Sync]: Resource[F, Blocker] =
    Blocker.fromExecutorService(this.newThreadFactory[F].map(tf => Executors.newCachedThreadPool(tf)))
}
