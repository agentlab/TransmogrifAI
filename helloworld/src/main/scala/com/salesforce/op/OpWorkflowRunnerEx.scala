package com.salesforce.op

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.avro.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import za.co.absa.abris.avro.read.confluent.{SchemaManagerFactory, SchemaManager}
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils

import com.salesforce.op.evaluators.{EvaluationMetrics, OpEvaluatorBase}
import com.salesforce.op.features.OPFeature
import com.salesforce.op.readers.{Reader, StreamingReader, CustomReader}
import com.salesforce.hw.titanic.Passenger2


class OpWorkflowRunnerEx (
  workflow: OpWorkflow,
  trainingReader: Reader[_],
  scoringReader: Reader[_],
  evaluationReader: Option[Reader[_]] = None,
  streamingScoreReader: Option[StreamingReader[_]] = None,
  evaluator: Option[OpEvaluatorBase[_ <: EvaluationMetrics]] = None,
  scoringEvaluator: Option[OpEvaluatorBase[_ <: EvaluationMetrics]] = None,
  featureToComputeUpTo: Option[OPFeature] = None
) extends OpWorkflowRunner(workflow, trainingReader, scoringReader, evaluationReader, streamingScoreReader, evaluator, scoringEvaluator, featureToComputeUpTo) {

  /**
   * This method is used to load up a previously trained workflow and use it to stream scores to a write location
   *
   * @param params    parameters injected at runtime
   * @param spark     spark context which runs the workflow
   * @param streaming spark streaming context which runs the workflow
   * @return StreamingScoreResult
   */
  override protected def streamingScore(params: OpParams) (implicit spark: SparkSession, streaming: StreamingContext): OpWorkflowRunnerResult = {
        
    // Prepare write path
    def writePath(timeInMs: Long) = Some(Paths.get(params.writeLocation.get, timeInMs.toString).toString)

    // Load the model to score with and prepare the scoring function
    val workflowModel = workflow.loadModel(params.modelLocation.get).setParameters(params)

    def scoreModel_DS(df: DataFrame, model: OpWorkflowModel): DataFrame = {
      import spark.implicits._
      println("to_ds")

      val ds: Dataset[Passenger2] = df.as[Passenger2]
      println("set_input_ds")
      val key: Passenger2 => String = _.PassengerId.toString

      val newReader = new CustomReader[Passenger2](key) {
        def readFn(params: OpParams)(implicit spark: SparkSession): Either[RDD[Passenger2], Dataset[Passenger2]] = Right(ds)

        /**
         * Generate the Dataframe that will be used in the OpPipeline calling this method
         *
         * @param rawFeatures features to generate from the dataset read in by this reader
         * @param opParams    op parameters
         * @param spark       spark instance to do the reading and conversion from RDD to Dataframe
         * @return A Dataframe containing columns with all of the raw input features expected by the pipeline
         */
        override def generateDataFrame(
          rawFeatures: Array[OPFeature],
          opParams: OpParams = new OpParams()
        )(implicit spark: SparkSession): DataFrame = {
          val rawData = read(opParams)
          val schema = getSchema(rawFeatures)

          rawData match {
            case Left(rdd) =>
              val d = rdd.flatMap(record => generateRow(key(record), record, rawFeatures, schema))
              spark.createDataFrame(d, schema)
            case Right(ds) =>
              val inputSchema = ds.schema.fields
              if (schema.forall(fn => inputSchema.exists( // check if features to be extracted already exist in dataframe
                fi => fn.name == fi.name && fn.dataType == fi.dataType && fn.nullable == fi.nullable)
              )) {
                val names = schema.fields.map(_.name).toSeq
                ds.select(names.head, names.tail: _*)
              } else {
                implicit val rowEnc = RowEncoder(schema)
                val df = ds.flatMap(record => generateRow(key(record), record, rawFeatures, schema))
                //spark.createDataFrame(df.rdd, schema) // because the spark row encoder does not preserve metadata
                df
              }
          }
        }
      }
      model.setReader(newReader)
      println("score_ds")
      val scoreModel =  model.scoreFn_Mock(keepRawFeatures = false, keepIntermediateFeatures = false, persistScores = false)(spark)
      val (scores: DataFrame, _) = scoreModel(None)
      scores
    }

    // Get the streaming score reader and create input stream
    //val reader = streamingScoreReader.get.asInstanceOf[StreamingReader[Any]]

    // This import is needed to use the $-notation
    import spark.implicits._

    val topicIn = "titanic-avro"
    val topicOut = "titanic-results-avro"
    val brokers = "localhost:9092"
    val schemaRegistryAddr = "http://localhost:8081"

    // https://issues.apache.org/jira/browse/SPARK-26314
    val abrisConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topicIn)
      .usingSchemaRegistry(schemaRegistryAddr)

    spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/SparkCheckpoints")

    val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topicIn)
        .load()
    
    println("from_avro")
    
    import za.co.absa.abris.avro.functions.{from_avro, to_avro}
    val output = df
        .select(from_avro($"value", abrisConfig) as 'data)
        .select("data.*")

    val scores = scoreModel_DS(output, workflowModel)
    scores.printSchema()

    val scoresFixed = scores
        .withColumnRenamed("age-cabin-embarked-name-pClass-parch-sex-sibSp-survived-ticket_6-stagesApplied_Prediction_000000000019", "Prediction")
    scoresFixed.printSchema()
    
    /*println("write_out_console")
    val query = scores
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", false)
        .start()
        .awaitTermination()*/

    println("write_out_kafka")

    // generate schema for all columns in a dataframe
    def schema = AvroSchemaUtils.toAvroSchema(scoresFixed)

    val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistryAddr)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)

    // register schema with topic name strategy
    def registerSchema1(schema: Schema, schemaManager: SchemaManager): Int = {
      val subject = SchemaSubject.usingTopicNameStrategy("topic", isKey=true) // Use isKey=true for the key schema and isKey=false for the value schema
      schemaManager.register(subject, schema)
    }

    val schemaId = registerSchema1(schema, schemaManager)

    def createConfig(schemaId: Int): ToAvroConfig = AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(schemaRegistryAddr)
    
    def toAvroConfig = createConfig(schemaId)

    val allColumns = struct(scoresFixed.columns.head, scoresFixed.columns.tail: _*)

    val query = scoresFixed
        .select(to_avro(allColumns, toAvroConfig) as 'value)
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", topicOut)
        .start()
        .awaitTermination()

    new StreamingScoreResult()
  }
}