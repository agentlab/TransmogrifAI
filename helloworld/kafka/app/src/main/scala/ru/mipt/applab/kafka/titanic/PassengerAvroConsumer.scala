package ru.mipt.applab.kafka.titanic

import ru.mipt.applab.kafka.common.{SerdeConfig}
import ru.mipt.applab.kafka.consumer.{
  AvroDeSerializer,
  ConsumerConfig,
  ConsumerUtils
}
import com.sksamuel.avro4s.RecordFormat
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.CollectionConverters.asJavaCollection
import scala.util.Try

object PassengerAvroConsumer
  extends App
  with ConsumerUtils[Passenger2]
  with AvroDeSerializer {

  private val (config, topic) = ConsumerConfig.getConfig("kafka-titanic-avro.conf")
  private val serde = SerdeConfig.getConfig("kafka-titanic-avro.conf")

  val keyDeSerializer: StringDeserializer = new StringDeserializer()

  implicit lazy val Valueformat: RecordFormat[Passenger2] = RecordFormat[Passenger2]
  val valueDeSerializer: Deserializer[Passenger2] = deserializer[Passenger2]
  valueDeSerializer.configure(serde, false)

  private val consumer = new KafkaConsumer(config, keyDeSerializer, valueDeSerializer)

  consumer.subscribe(asJavaCollection(List(topic)))

  consumer.seekToBeginning(Nil.asJava)

  Try {
    while (true) {
      val messages = pool(consumer, FiniteDuration(1, MILLISECONDS))

      for ((_, passenger) <- messages) {
        logger.info(
          s"New passenger received. PassengerId: ${passenger.PassengerId} .  Name: ${passenger.Name}, Sex: ${passenger.Sex}  "
        )
      }
    }
  }.recover { case error =>
    logger.error(error)
    logger.error(
      "Something went wrong when seeking messsages from begining. Unsubscribing"
    )
    consumer.unsubscribe();
  }
  consumer.close()
}
