package com.salesforce.op.utils.cache

import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.reflect.ClassTag
import org.slf4j.LoggerFactory
import org.apache.spark.storage.StorageLevel

object CacheUtils {

  private val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  val DEFAULT_CONTEXT_NAME = "default"

  private val persistedDatasets = mutable.Map[String, mutable.Set[Dataset[_]]]()
  private val persistedRDDs = mutable.Map[String, mutable.Set[RDD[_]]]()

  def cache[T: ClassTag](dataset: Dataset[T]): Dataset[T] =
    cache(dataset, DEFAULT_CONTEXT_NAME, StorageLevel.MEMORY_AND_DISK)

  def cache[T: ClassTag](dataset: Dataset[T], context: String): Dataset[T] =
    cache(dataset, context, StorageLevel.MEMORY_AND_DISK)

  def cache[T : ClassTag](
    dataset: Dataset[T],
    context: String,
    level: StorageLevel): Dataset[T] = {
    val contextDatasets = persistedDatasets.getOrElseUpdate(context, mutable.Set.empty)
    if (contextDatasets(dataset)) {
      log.warn(s"[${context}] Dataset ${dataset.hashCode()} is already cached")
      dataset
    } else {
      dataset.persist(level)
      contextDatasets.add(dataset)
      log.info(s"[${context}] Persisted dataset: ${dataset.hashCode()}")
      dataset
    }
  }

  def cache[T: ClassTag](rdd: RDD[T]): RDD[T] = cache(rdd, DEFAULT_CONTEXT_NAME)

  def cache[T : ClassTag](
    rdd: RDD[T],
    context: String,
    level: StorageLevel = StorageLevel.MEMORY_AND_DISK): RDD[T] = {
    val contextRDDs = persistedRDDs.getOrElseUpdate(context, mutable.Set.empty)
    if (contextRDDs(rdd)) {
      rdd
    } else {
      rdd.persist(level)
      contextRDDs.add(rdd)
      log.info(s"[${context}] Persisted RDD: ${rdd.id}")
      rdd
    }
  }

  def uncache(dataset: Dataset[_]): Unit = uncache(dataset, DEFAULT_CONTEXT_NAME)

  def uncache(dataset: Dataset[_], context: String): Unit = {
    val contextDatasets = persistedDatasets.getOrElseUpdate(context, mutable.Set.empty)
    if (contextDatasets(dataset)) {
      dataset.unpersist()
      log.info(s"[${context}] Dataset ${dataset.hashCode()} removed from cache")
      contextDatasets.remove(dataset)
    } else {
      log.warn(s"[${context}] No persisted dataset ${dataset.hashCode()} found")
    }
  }

  def uncache(rdd: RDD[_]): Unit = uncache(rdd, DEFAULT_CONTEXT_NAME)

  def uncache(rdd: RDD[_], context: String): Unit = {
    val contextRDDs = persistedRDDs.getOrElseUpdate(context, mutable.Set.empty)
    if (contextRDDs(rdd)) {
      rdd.unpersist()
      log.info(s"[${context}] RDD ${rdd.id} removed from cache")
      contextRDDs.remove(rdd)
    } else {
      log.warn(s"[${context}] No persisted RDD ${rdd.id} found")
    }
  }

  def clearContext(ctx: String): Unit = {
    val contextDatasets = persistedDatasets.getOrElseUpdate(ctx, mutable.Set.empty)
    contextDatasets.foreach(ds => {
      ds.unpersist()
      log.info(s"[${ctx}] Dataset ${ds.hashCode()} removed from cache")
    })
    contextDatasets.clear()
    val contextRDDs = persistedRDDs.getOrElseUpdate(ctx, mutable.Set.empty)
    contextRDDs.foreach(rdd => {
      rdd.unpersist()
      log.info(s"[${ctx}] RDD ${rdd.id} removed from cache")
    })
    contextRDDs.clear()
  }

  def clearCache(context: Option[String] = None): Unit = {
    context match {
      case None => (persistedDatasets.keySet ++ persistedRDDs.keySet).foreach(clearContext)
      case Some(ctx) => clearContext(ctx)
    }
  }

  def clearCacheKeepWithContext(
    context: String,
    keepDatasets: Seq[Dataset[_]] = Seq.empty,
    keepRDDs: Seq[RDD[_]] = Seq.empty): Unit = {
      val contextDatasets = persistedDatasets.getOrElseUpdate(context, mutable.Set.empty)
      contextDatasets.filterNot(keepDatasets.contains(_)).foreach(uncache(_, context))
      val contextRDDs = persistedRDDs.getOrElseUpdate(context, mutable.Set.empty)
      contextRDDs.filterNot(keepRDDs.contains(_)).foreach(uncache(_, context))
  }

  def clearCacheKeep(keepDatasets: Seq[Dataset[_]] = Seq.empty, keepRDDs: Seq[RDD[_]] = Seq.empty): Unit = {
    persistedDatasets.foreach(ctx => {
      val datasetsToUnpersist = ctx._2.filterNot(keepDatasets.contains(_))
      datasetsToUnpersist.foreach(uncache(_, ctx._1))
    })
    persistedRDDs.foreach(ctx => {
      val rddsToUnpersist = ctx._2.filterNot(keepRDDs.contains(_))
      rddsToUnpersist.foreach(uncache(_, ctx._1))
    })
  }

  def checkpoint[T: ClassTag](dataset: Dataset[T], context: String): (Dataset[T], Seq[RDD[_]]) = {
    val sparkContext = dataset.sparkSession.sparkContext
    val persistedRDDsBefore = sparkContext.getPersistentRDDs.keySet
    val checkpointedDf = dataset.localCheckpoint()
    val persistedRDDsAfter = sparkContext.getPersistentRDDs.keySet
    val rddsToPersist = persistedRDDsAfter.diff(persistedRDDsBefore).map(sparkContext.getPersistentRDDs)
    val contextRDDs = persistedRDDs.getOrElseUpdate(context, mutable.Set.empty)
    rddsToPersist.foreach(rdd => {
      contextRDDs.add(rdd)
      log.info(s"[$context] persisted RDD: ${rdd.id}")
    })
    (checkpointedDf, rddsToPersist.toSeq)
  }
}
