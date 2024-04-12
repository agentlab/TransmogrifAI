package ru.mipt.applab.kafka.titanic

import ru.mipt.applab.kafka.common.{SerdeConfig}
import ru.mipt.applab.kafka.producer.{
  AvroSerializer,
  ProducerConfig,
  ProducerUtils
}
import com.sksamuel.avro4s.RecordFormat
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

import scala.util.Try


object PassengerAvroProducer
  extends App
  with ProducerUtils[Passenger2]
  with AvroSerializer {

  private val (config, topic) = ProducerConfig.getConfig("kafka-titanic-avro.conf")
  private val serde = SerdeConfig.getConfig("kafka-titanic-avro.conf")

  val keySerializer: StringSerializer = new StringSerializer()

  implicit lazy val Valueformat: RecordFormat[Passenger2] = RecordFormat[Passenger2]
  val valueSerializer: Serializer[Passenger2] = AvroSerializer[Passenger2]
  valueSerializer.configure(serde, false)

  private val producer = new KafkaProducer(config, keySerializer, valueSerializer)
  private val passengers = Generator.passengers

  Try {
    //producer.initTransactions()
    //producer.beginTransaction()
    for (passenger <- passengers) {
      produce(producer, topic, passenger.PassengerId.toString, passenger)
    }
    //producer.commitTransaction()
    logger.info("Successfully completed kafka transaction.")
  }.recover { case error =>
    logger.error(error)
    logger.error(
      "Something went wrong during kafka transcation processing. Aborting"
    )
    //producer.abortTransaction();
  }
  producer.close()

}