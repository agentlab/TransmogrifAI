package com.salesforce.op.utils.cache

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.reflect.ClassTag
import org.slf4j.LoggerFactory

object CacheUtils {

  private val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val persistedDatasets = mutable.Set.empty[Dataset[_]]
  private val persistedRDDs = mutable.Set.empty[RDD[_]]

  def cache[T : ClassTag](dataset: Dataset[T]): Dataset[T] = {
    if (persistedDatasets(dataset)) {
      log.warn(s"Dataset ${dataset} is already cached")
      dataset
    } else {
      val cachedDataset = dataset.persist()
      persistedDatasets += cachedDataset
      log.info(s"Persisted dataset: ${dataset}")
      cachedDataset
    }
  }

  def cache[T : ClassTag](rdd: RDD[T]): RDD[T] = {
    if (persistedRDDs(rdd)) {
      rdd
    } else {
      val cachedRDD = rdd.persist()
      persistedRDDs += cachedRDD
      log.info(s"Persisted RDD: ${rdd.id}")
      cachedRDD
    }
  }

  def uncache(dataset: Dataset[_]): Unit = {
    if (persistedDatasets(dataset)) {
      dataset.unpersist()
    } else {
      log.warn(s"No persisted dataset ${dataset} found")
    }
  }

  def uncache(rdd: RDD[_]): Unit = {
    if (persistedRDDs(rdd)) {
      rdd.unpersist()
    } else {
      log.warn(s"No persisted RDD ${rdd.id} found")
    }
  }

  def clearCache(): Unit = {
    persistedDatasets.foreach(_.unpersist())
    persistedRDDs.foreach(_.unpersist())
  }
}
