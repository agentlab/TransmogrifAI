package ru.mipt.applab.kafka.article

import java.time.LocalDate

case class Article(
  id: String,
  title: String,
  content: String,
  created: LocalDate,
  author: Author
)

case class Author(id: Int, name: String)