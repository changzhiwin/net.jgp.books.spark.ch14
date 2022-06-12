

import org.scalatest._
import flatspec._
import matchers._
import java.sql.Timestamp

import net.jgp.books.spark.utils.DateUtils

// https://blog.rockthejvm.com/testing-styles-scalatest/
// https://www.scalatest.org/quick_start
class DateUtilsTest extends AnyFlatSpec with should.Matchers {

  val weekDays = Seq(
    "10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00",
    "10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00",
    "09:45-13:00 and 14:00-17:00 ",
    "09:45-13:00 and 14:00-17:00 ",
    "09:45-16:30 ",
    "09:45-16:30 ",
    "Closed",
  )

  "DateUtilsTest" should "open at 06-06 15:05:00" in {

    val dateTime = Timestamp.valueOf("2022-06-06 15:05:00")

    DateUtils.isOpen(dateTime, weekDays: _*) should be (true)
  }

  it should "close at 07-06 16:05:00" in {

    val dateTime = Timestamp.valueOf("2021-07-06 16:05:00")

    DateUtils.isOpen(dateTime, weekDays: _*) should be (false)
  }

  it should "open at 07-09 14:15:00" in {

    val dateTime = Timestamp.valueOf("2021-07-09 14:15:00")

    DateUtils.isOpen(dateTime, weekDays: _*) should be (true)
  }

  it should "close at 07-04 14:05:00" in {

    val dateTime = Timestamp.valueOf("2021-07-04 14:05:00")

    DateUtils.isOpen(dateTime, weekDays: _*) should be (false)
  }

}