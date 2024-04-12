package ru.mipt.applab.kafka.titanic

case class Passenger2(PassengerId: Int, Survived: Int, Pclass: Int, Name: String, Sex: String, Age: Option[Double], SibSp: Int, Parch: Int, Ticket: String, Fare: Double, Cabin: Option[String], Embarked: String)
