package ru.mipt.applab.kafka.titanic

object Generator {
  def passengers: List[Passenger2] = {
    List(
      Passenger2(1,0,3,"Braund, Mr. Owen Harris","male",Some(22),1,0,"A/5 21171", 7.25, None, "S"),
      Passenger2(2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",Some(38),1,0,"PC 17599",71.2833,Some("C85"),"C")
    )
  }
}