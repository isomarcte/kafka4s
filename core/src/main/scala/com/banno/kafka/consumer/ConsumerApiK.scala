package com.banno.kafka.consumer

import cats._
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._
import scala.concurrent.duration._

/** An implementation of [[ConsumerApi]] which proxies calls to an wrapped
  * instance, transforming the result with a natural
  * transformation, i.e. `cats.arrow.FunctionK`.
  *
  * This makes implementing transforms in the effect layer very simple. See
  * [[ShiftingConsumerApi]] and [[SynchronizedConsumerApi]] for examples.
  *
  * @note While the natural transformation is defined in terms of F ~> G, in
  *       practice it will often be F ~> F.
  */
trait ConsumerApiK[F[_], G[_], K, V] extends ConsumerApi[G, K, V] {

  protected def nat: F ~> G
  protected def backingConsumer: ConsumerApi[F, K, V]

  override def assign(partitions: Iterable[TopicPartition]): G[Unit] =
    this.nat(this.backingConsumer.assign(partitions))
  override def assignment: G[Set[TopicPartition]] =
    this.nat(this.backingConsumer.assignment)
  override def beginningOffsets(partitions: Iterable[TopicPartition]): G[Map[TopicPartition, Long]] =
    this.nat(this.backingConsumer.beginningOffsets(partitions))
  override def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): G[Map[TopicPartition, Long]] =
    this.nat(this.backingConsumer.beginningOffsets(partitions, timeout))
  override def close: G[Unit] =
    this.nat(this.backingConsumer.close)
  override def close(timeout: FiniteDuration): G[Unit] =
    this.nat(this.backingConsumer.close(timeout))
  override def commitAsync: G[Unit] =
    this.nat(this.backingConsumer.commitAsync)
  override def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): G[Unit] =
    this.nat(this.backingConsumer.commitAsync(offsets, callback))
  override def commitAsync(callback: OffsetCommitCallback): G[Unit] =
    this.nat(this.backingConsumer.commitAsync(callback))
  override def commitSync: G[Unit] =
    this.nat(this.backingConsumer.commitSync)
  override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): G[Unit] =
    this.nat(this.backingConsumer.commitSync(offsets))
  override def committed(partition: Set[TopicPartition]): G[Map[TopicPartition, OffsetAndMetadata]] =
    this.nat(this.backingConsumer.committed(partition))
  override def endOffsets(partitions: Iterable[TopicPartition]): G[Map[TopicPartition, Long]] =
    this.nat(this.backingConsumer.endOffsets(partitions))
  override def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): G[Map[TopicPartition, Long]] =
    this.nat(this.backingConsumer.endOffsets(partitions, timeout))
  override def listTopics: G[Map[String, Seq[PartitionInfo]]] =
    this.nat(this.backingConsumer.listTopics)
  override def listTopics(timeout: FiniteDuration): G[Map[String, Seq[PartitionInfo]]] =
    this.nat(this.backingConsumer.listTopics(timeout))
  override def metrics: G[Map[MetricName, Metric]] =
    this.nat(this.backingConsumer.metrics)
  override def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): G[Map[TopicPartition, OffsetAndTimestamp]] =
    this.nat(this.backingConsumer.offsetsForTimes(timestampsToSearch))
  override def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): G[Map[TopicPartition, OffsetAndTimestamp]] =
    this.nat(this.backingConsumer.offsetsForTimes(timestampsToSearch, timeout))
  override def partitionsFor(topic: String): G[Seq[PartitionInfo]] =
    this.nat(this.backingConsumer.partitionsFor(topic))
  override def partitionsFor(topic: String, timeout: FiniteDuration): G[Seq[PartitionInfo]] =
    this.nat(this.backingConsumer.partitionsFor(topic, timeout))
  override def pause(partitions: Iterable[TopicPartition]): G[Unit] =
    this.nat(this.backingConsumer.pause(partitions))
  override def paused: G[Set[TopicPartition]] =
    this.nat(this.backingConsumer.paused)
  override def poll(timeout: FiniteDuration): G[ConsumerRecords[K, V]] =
    this.nat(this.backingConsumer.poll(timeout))
  override def position(partition: TopicPartition): G[Long] =
    this.nat(this.backingConsumer.position(partition))
  override def resume(partitions: Iterable[TopicPartition]): G[Unit] =
    this.nat(this.backingConsumer.resume(partitions))
  override def seek(partition: TopicPartition, offset: Long): G[Unit] =
    this.nat(this.backingConsumer.seek(partition, offset))
  override def seekToBeginning(partitions: Iterable[TopicPartition]): G[Unit] =
    this.nat(this.backingConsumer.seekToBeginning(partitions))
  override def seekToEnd(partitions: Iterable[TopicPartition]): G[Unit] =
    this.nat(this.backingConsumer.seekToEnd(partitions))
  override def subscribe(topics: Iterable[String]): G[Unit] =
    this.nat(this.backingConsumer.subscribe(topics))
  override def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): G[Unit] =
    this.nat(this.backingConsumer.subscribe(topics))
  override def subscribe(pattern: Pattern): G[Unit] =
    this.nat(this.backingConsumer.subscribe(pattern))
  override def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): G[Unit] =
    this.nat(this.backingConsumer.subscribe(pattern, callback))
  override def subscription: G[Set[String]] =
    this.nat(this.backingConsumer.subscription)
  override def unsubscribe: G[Unit] =
    this.nat(this.backingConsumer.unsubscribe)
  override def wakeup: G[Unit] =
    this.nat(this.backingConsumer.wakeup)
}
