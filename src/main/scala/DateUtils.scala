package net.jgp.books.spark.utils

import java.util.Locale
import java.sql.Timestamp

import org.apache.spark.sql.types.{ DataType }

import com.github.nscala_time.time.Imports.{ DateTime }

object DateUtils {

  // one day's open time, by second format[0, 1440)
  case class OpenTimeBySecond(start: Int, end: Int) {
    def checkOpen(t: Int): Int = {
      if (start <= t && t <= end) 1
      else 0
    }
  }

  case class OpenState(desc: String, weekDay: Int, answer: Boolean)

  def getDetailUDF(hoursMon: String, hoursTue: String, 
      hoursWed: String, hoursThu: String, hoursFri: String, 
      hoursSat: String, hoursSun: String, dateTime: Timestamp
  ): OpenState = { //Map[String, String] = { //(String, Int, Boolean) = {
    val (_, weekDay, _) = getMomentHHmm(dateTime)

    val desc = Seq(hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun)(weekDay - 1)

    val open = isOpen(dateTime, hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun)
    
    // (desc, weekDay, open)
    // Map("desc" -> desc, "weekDay" -> weekDay.toString, "answer" -> open.toString)
    OpenState(desc, weekDay, open)
  }

  def isOpenUDF(hoursMon: String, hoursTue: String, 
      hoursWed: String, hoursThu: String, hoursFri: String, 
      hoursSat: String, hoursSun: String, dateTime: Timestamp
  ): Boolean = {
    isOpen(dateTime, hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun)
  }

  // weekDays must order by nuture(Mon, Tue...)
  def isOpen(dateTime: Timestamp, weekDays: String*): Boolean = {
    val (moment, weekDay, month) = getMomentHHmm(dateTime)

    val weekDaySeq = weekDays.toSeq.map(s => s.trim)

    // weekDay and month format: https://www.joda.org/joda-time/field.html
    weekDay match {
      case weekIdx: Int if (weekIdx > 0 && weekIdx < 8) => betweenTheOpenTime(moment, weekDaySeq(weekIdx - 1), month)
      case _  =>  false
    }
  }

  private def betweenTheOpenTime(moment: String, openDescByWeekDay: String, whichMonth: String): Boolean = {
    val periods = parseOpenTimeFormat(openDescByWeekDay, whichMonth)

    val momentInt = transfromHHmmToSeconds(moment)

    val ret = periods.foldLeft(0)((open, t) => open + t.checkOpen(momentInt))

    ret > 0
  }

  private def parseOpenTimeFormat(desc: String, whichMonth: String): Seq[OpenTimeBySecond] = {
    // 10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00
    // 09:45-13:00 and 14:00-17:00 
    // Closed
    // 09:45-16:30 
    desc match {
      case s"$normal ($specialClosed $atMonths) - closed for lunch $lunch" => handleSpecialClosed(normal, specialClosed, atMonths, lunch, whichMonth)
      case s"$morning and $afternoon"                                      => handleMorAndAfter(morning, afternoon)
      case "Closed"                                                        => Seq.empty
      case period: String                                                  => Seq(handleSinglePeriod(period))
    }
  }

  private def handleSpecialClosed(normal: String, specialClosed: String, atMonths: String, lunch: String, whichMonth: String): Seq[OpenTimeBySecond] = {
    //("July", "August")
    val specMonths = atMonths.split("and").map(m => m.trim)

    // (10:00, 16:00)
    val workTimes = if (specMonths.contains(whichMonth)) {
      Seq(normal.split("-")(0), specialClosed)
    } else {
      normal.split("-").toSeq
    }

    // (12:30, 13:00)
    val lunchTimes = lunch.split("-").toSeq

    // (10:00-12:30)
    val morning   = Seq(workTimes(0), lunchTimes(0)).mkString("-")
    // (13:00-16:00)
    val afternoon = Seq(lunchTimes(1), workTimes(1)).mkString("-")

    handleMorAndAfter(morning, afternoon)
  }

  private def handleMorAndAfter(morning: String, afternoon: String): Seq[OpenTimeBySecond] = {
    Seq(handleSinglePeriod(morning), handleSinglePeriod(afternoon))
  }

  private def handleSinglePeriod(period: String): OpenTimeBySecond = {
    val startAndEnd = period.split("-")
    val (start, end) = (startAndEnd(0), startAndEnd(1))
    OpenTimeBySecond(transfromHHmmToSeconds(start), transfromHHmmToSeconds(end))
  }

  private def transfromHHmmToSeconds(hhMM: String): Int = {
    val arr = hhMM.split(":").map(n => n.toInt)
    arr(0) * 60 + arr(1)
  }

  private def getMomentHHmm(dateTime: Timestamp): (String, Int, String) = {
    val dt = new DateTime(dateTime.getTime)
    val moment = dt.toString("HH:mm")
    val weekDay = dt.getDayOfWeek
    val month = dt.monthOfYear.getAsText(Locale.ENGLISH) // June July August

    //("16:30", 4, "June")
    (moment, weekDay, month)
  }
}