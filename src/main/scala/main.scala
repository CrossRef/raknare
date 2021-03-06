package org.crossref.räknare

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD}
import net.liftweb.json._
import net.liftweb.json.Serialization.{read, write}
import java.net.URL
import java.util.{Arrays, Calendar, Locale}
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import math.max 
import java.util.concurrent.TimeUnit
import java.io.File

case class Date(_year: Int, _month: Int, _day: Int) {
  def year: String = {
    _year.toString()
  }

  def yearMonth: String = {
    _year.toString() + "-" + _month.toString()
  }

  
  def yearMonthDay: String = {
    _year.toString() + "-" + _month.toString() + "-" + _day.toString()
  }
}

case class DomainTriple(subdomain: String, domainName: String, etld: String) {
  def fullDomain = {
    if (subdomain.isEmpty) {
      domainName + "." + etld
    } else {
      subdomain + "." + domainName + "." + etld
    }
  }

  def domain = {
    domainName + "." + etld
  }
}

case class LogLine(date: Date, doi: String, referrer: DomainTriple, status: String)


object Main {
  def lineRe = "^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$".r

  def stripQuotes(input: String) = {
    if (input.startsWith("\"") && input.endsWith("\"")) {
        input.substring(1, input.length - 1)
      } else {
        input
      }
  }
  
  def constructDomain(url: String) : DomainTriple = {
    try {
      if (url.trim().length == 0) {
        DomainTriple("", "no-reffer", "")
      } else if (url.startsWith("file:")) {
        DomainTriple("", "local-file", "") 
      } else {
        val host = new URL(url).getHost().toLowerCase()
        return ETld.split(host)  
      }
    } catch {
      case e: java.net.MalformedURLException => {
        // e.g. ""
        DomainTriple("", "malformed", "")
      }
      case e: java.lang.ArrayIndexOutOfBoundsException => {
        // e.g. http://http://dx.doi.org/10.1016/S0147-1767(01)00023-2?nosfx=y&id=2010123075&sinbun=4800&prmstaging=&pds_handle=210201215049648310120290089693887&calling_system=primo
        DomainTriple("", "malformed", "") 
      }
      case e: java.lang.NullPointerException => {
        // e.g. http://///fQMAAAAAAAAFAAAABmlfMzYwaQpTUzJDWTZWUDRRBmNfb3ZlcgExBGlfZmsABmlfMzYwYwExB2lfMzYwZXoFZmFsc2UAAAAA
        println("***ERROR***")
        println(url)

        DomainTriple("", "malformed", "")
      }
      case e: java.lang.Exception => {
        // everything else. After all, any string could be sent.
        println("***ERROR***")
        println(url)

        DomainTriple("", "malformed", "")
      }
    }
  }

  def parseDate(input: String) = {
    val dateFormat1 = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy")
    val dateFormat2 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    val dateFormat3 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'")

    val parsed = try {
      dateFormat1.parse(input)
      } catch {
        case e: java.text.ParseException => {
          try {
            dateFormat2.parse(input)
          } catch {
            case e: java.text.ParseException => {
              dateFormat3.parse(input)
            } case e: java.text.ParseException => {
              println("Fail date parse")
              println(input)
              throw(e)
            }
          }
        }
      }
    
    val calendar = Calendar.getInstance
    calendar.setTime(parsed)
    
    val year = calendar.get(Calendar.YEAR)
    val month = calendar.get(Calendar.MONTH) + 1
    val day = calendar.get(Calendar.DAY_OF_MONTH)

    Date(year, month, day)

  }

  // Return a full or empty sequence.
  def parseLine(line: String) : Seq[LogLine] = {
    // println("parse" + line)
    // try {
      val pattern = lineRe.findFirstMatchIn(line)
      
      pattern match {
        case Some(matchedLine) => {
          val dateStr = stripQuotes(matchedLine.group(3))
          val doi = stripQuotes(matchedLine.group(7))
          val status = stripQuotes(matchedLine.group(8))
          val referrer = stripQuotes(matchedLine.group(9))

          val date = parseDate(dateStr)
          val domainTriple : DomainTriple = constructDomain(referrer);

          List(LogLine(date, doi, domainTriple, status))
        }
        case None => List()
      }
    // } catch {
      // case e: Throwable=> {
        // println("********************************************")
        // println(line)
        // List()
      // }
    // }  
  }

  // Parse lines. Immediately filter out dates that are out of the date range (there will be up to 1 month either side).
  def parseLines(rdd : RDD[String], startYearMonth: Tuple2[Int, Int], endYearMonth: Tuple2[Int, Int]) = {
    val startMonth = startYearMonth._1 * 12 + startYearMonth._2;
    val endMonth = endYearMonth._1 * 12 + endYearMonth._2;

    val lines = rdd.flatMap(parseLine)

    lines.filter(line => (line.date._year * 12 + line.date._month) >= startMonth && (line.date._year * 12 + line.date._month) <= endMonth)
  }

  def tupleToVector(value: Tuple2[Any, Any]) = {
    Vector(value._1, value._2)
  }

  def unparse(line: AnyRef): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    write(line)
  }

  def count(lines: RDD[_]) = {
    lines.map(x => (x, 1)).reduceByKey(_ + _)
  }

  // TASKS

  // "www.xyz.com"
  def fullDomainAllTime(lines: RDD[LogLine], outputDir: String) {
    val projection = lines.map(line => line.referrer.fullDomain)
    val counted = count(lines)
    counted.map(tupleToVector).map(unparse).saveAsTextFile(outputDir + "/fullDomainCount")

  }

  // "xyz.com"
  def domainAllTime(lines: RDD[LogLine], outputDir: String) {
    val projection = lines.map(line => line.referrer.domain)
    val counted = count(lines)
    counted.map(tupleToVector).map(unparse).saveAsTextFile(outputDir + "/domainCount")
  }

  // "xyz"
  def domainNameAllTime(lines: RDD[LogLine], outputDir: String) {
    val projection = lines.map(line => line.referrer.domainName)
    val counted = count(lines)
    counted.map(tupleToVector).map(unparse).saveAsTextFile(outputDir + "/domainNameCount")
  }

  // "10.5555/12345678"
  def doiAllTime(lines: RDD[LogLine], outputDir: String) {
    val projection = lines.map(line => line.doi)
    val counted = count(lines)
    counted.map(tupleToVector).map(unparse).saveAsTextFile(outputDir + "/doiCount")
  }

  // "10.5555/12345678", "xyz.com"
  def doiDomainAllTime(lines: RDD[LogLine], outputDir: String) {
    val projection = lines.map(line => (line.doi, line.referrer.domain))
    val counted = count(lines)
    counted.map(tupleToVector).map(unparse).saveAsTextFile(outputDir + "/doiCount")
  } 

  // Per period

  def fullDomainPeriod(lines: RDD[LogLine], period: Symbol, outputDir: String) {
    val projection = period match {
      case 'year => lines.map(line => (line.referrer.fullDomain, line.date.year))
      case 'month => lines.map(line => (line.referrer.fullDomain, line.date.yearMonth))
      case 'day => lines.map(line => (line.referrer.fullDomain, line.date.yearMonthDay))
    }

    val counted = count(projection)
    counted.map{case ((domain, period), count) => Vector(domain, period, count)}.map(unparse).saveAsTextFile(outputDir + "/fullDomain-" + period.name)
  }

  def domainPeriod(lines: RDD[LogLine], period: Symbol, outputDir: String) {
    val projection = period match {
      case 'year => lines.map(line => (line.referrer.domain, line.date.year))
      case 'month => lines.map(line => (line.referrer.domain, line.date.yearMonth))
      case 'day => lines.map(line => (line.referrer.domain, line.date.yearMonthDay))
    }

    val counted = count(projection)
    counted.map{case ((domain, period), count) => Vector(domain, period, count)}.map(unparse).saveAsTextFile(outputDir + "/domain-" + period.name)
  }

  def domainNamePeriod(lines: RDD[LogLine], period: Symbol, outputDir: String) {
    val projection = period match {
      case 'year => lines.map(line => (line.referrer.domainName, line.date.year))
      case 'month => lines.map(line => (line.referrer.domainName, line.date.yearMonth))
      case 'day => lines.map(line => (line.referrer.domainName, line.date.yearMonthDay))
    }

    val counted = count(projection)
    counted.map{case ((domain, period), count) => Vector(domain, period, count)}.map(unparse).saveAsTextFile(outputDir + "/domainName-" + period.name)
  }

  def doiPeriod(lines: RDD[LogLine], period: Symbol, outputDir: String) {
    val projection = period match {
      case 'year => lines.map(line => (line.doi, line.date.year))
      case 'month => lines.map(line => (line.doi, line.date.yearMonth))
      case 'day => lines.map(line => (line.doi, line.date.yearMonthDay))
    }

    val counted = count(projection)
    counted.map{case ((doi, period), count) => Vector(doi, period, 999)}.map(unparse).saveAsTextFile(outputDir + "/räknare/doi-" + period.name)
  }

  def doiDomainPeriod(lines: RDD[LogLine], period: Symbol, outputDir: String) {
    val projection = period match {
      case 'year => lines.map(line => (line.doi, line.referrer.domain, line.date.year))
      case 'month => lines.map(line => (line.doi, line.referrer.domain, line.date.yearMonth))
      case 'day => lines.map(line => (line.doi, line.referrer.domain, line.date.yearMonthDay))
    }

    val counted = count(projection)
    counted.map{case ((doi, domain, period), count) => Vector(doi, domain, period, count)}.map(unparse).saveAsTextFile(outputDir + "/doiDomain-" + period.name)
  }

  def statusPeriod(lines: RDD[LogLine], period: Symbol, outputDir: String) {
    val projection = period match {
      case 'year => lines.map(line => (line.status, line.date.year))
      case 'month => lines.map(line => (line.status, line.date.yearMonth))
      case 'day => lines.map(line => (line.status, line.date.yearMonthDay))
    }

    val withStatus = projection.map{case (status, period) => (status.split("/")(0).toLowerCase(), period)}

    val counted = count(withStatus)

    counted.map{case ((status, period), count) => Vector(status, period, count)}.map(unparse).saveAsTextFile(outputDir + "/status-" + period.name)
  }

  def topDomainsPeriod(lines: RDD[LogLine], period: Symbol, outputDir: String) {
    val projection = period match {
      case 'year => lines.map(line => (line.referrer.domain, line.date.year))
      case 'month => lines.map(line => (line.referrer.domain, line.date.yearMonth))
      case 'day => lines.map(line => (line.referrer.domain, line.date.yearMonthDay))
    }

    // Into RDD of ((domain, period), count).
    val counted = count(projection)

    
    val maxCount = 100
    val threshold = 100

    // Into KV pair or (period, (domain, count))
    val grouped = counted.map{case ((domain, period), count) => (period, (domain, count))}.groupByKey()

    // Accumulator of size of table, minimum count in table, vector of value and count.
    case class TopNCount (size: Int, minCount: Int, table: List[Tuple2[Any, Int]])

    val initialValue = TopNCount(0,0, List())

    val topN = grouped.aggregateByKey(initialValue)((acc: TopNCount, values) => {
        
        // Immediately throw out values under threshold.
        // Insert the top maxCount values into the acc table.
        values.filter(value => value._2 > threshold).foldLeft(acc){
          (innerAcc, value) => {
            // For the first maxCount values, just fill up the table.
            if (innerAcc.size < maxCount) {
              TopNCount(innerAcc.size + 1, value._2, innerAcc.table :+ value)
            // After that, only replace values that fall in the top maxCount.
            } else {
              if (value._2 > innerAcc.minCount) {
                var newTable = (innerAcc.table :+ value).sortBy(_._2).reverse.take(maxCount)
                var newMin = newTable.minBy(_._2)._2
                TopNCount(innerAcc.size, newMin, newTable)
              } else {
                innerAcc
              }
            }
          }
        }
      }, (acc1, acc2) => {
        // Merge two tables to preserve top maxCount items.
        var newTable = (acc1.table ++ acc2.table).sortBy(_._2).reverse.take(maxCount)
        var newMin =  math.max(acc1.minCount, acc2.minCount)

        TopNCount(maxCount, newMin, newTable)
      })

    topN.map{case (period, values) => Vector(period, values.table.map(tupleToVector))}.map(unparse).saveAsTextFile(outputDir + "/topDomains-" + period.name)
  }

  // Tuples of domain and domainName, domain and fullDomain. For easy look-ups later.
  def domainList(lines: RDD[LogLine], outputDir: String) {
    val domains = lines.map(line => line.referrer.domainName).distinct();
    val domainNames = lines.map(line => (line.referrer.domain, line.referrer.domainName)).distinct();
    val fullDomains = lines.map(line => (line.referrer.fullDomain, line.referrer.domainName)).distinct();

    domains.map(unparse).saveAsTextFile(outputDir + "/domainList")
    domainNames.map(tupleToVector).map(unparse).saveAsTextFile(outputDir + "/domainNameList")
    fullDomains.map(tupleToVector).map(unparse).saveAsTextFile(outputDir + "/fullDomainList")
  }

  // UTILS

  def parseBoundaryDate(input: String) = {
    val parts = input.split("-")
    (Integer.parseInt(parts(0)), Integer.parseInt(parts(1)))
  }

  // Sequence of year-month components as found in path.
  def dateRangePaths(start: Tuple2[Int, Int], end: Tuple2[Int, Int], base: String) = {
    // Extend range by 1 either side. This is because of issues caused by servers being in different timezones, truncating log files at different points in time.
    val startMonth = (start._1 * 12 + start._2) - 2
    val endMonth = (end._1 * 12 + end._2) + 1

    // Log naming changes at some point in history.
    val paths = for (month <- startMonth until endMonth) yield base + "access?log?%04d%02d*".format(month / 12, (month % 12) + 1)

    paths.mkString(",")
  }

  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf()

    // val sc = new SparkContext("local[1]", "Räknare Test", sparkConf)
    val sc = try {
      new SparkContext(sparkConf)
      } catch {
        case e: org.apache.spark.SparkException => {
          println("Running in local mode")
          new SparkContext("local[1]", "Räknare Test", sparkConf)
        }
      }      

    // val inputPath = sparkConf.get("spark.raknare.inputfiles")
    val inputDir = sparkConf.get("spark.raknare.inputdir")
    val outputDir = sparkConf.get("spark.raknare.outputdir")
    
    // Produce a sequence of date components that can be put into paths.
    val startDate = parseBoundaryDate(sparkConf.get("spark.raknare.startdate"))
    val endDate = parseBoundaryDate(sparkConf.get("spark.raknare.enddate"))
    val paths = dateRangePaths(startDate, endDate, inputDir)

    println("INPUT PATHS " +  paths)

    val logFileInputs = sc.textFile(paths)
  
    // Flatmap because some lines will fail to parse, returning an empty result. Therefore `parse` returns a 0 or 1-length Seq.
    val lines = parseLines(logFileInputs, startDate, endDate).persist(StorageLevel.DISK_ONLY)

    /*
     * Aggregate.
     */
     val forPeriod = sparkConf.get("spark.raknare.period", "month") match {
        case "year" => 'year
        case "month" => 'month
        case "day" => 'day
     }

     println("PERIOD", forPeriod)

     val tasks = sparkConf.get("spark.raknare.tasks", "").split(",")

     println("TASKS", tasks.deep.mkString(" "))

     if (tasks.contains("domainList")) {
      println("Task: Domain list")
      domainList(lines, outputDir)
     }

     if (tasks.contains("status")) {
      println("Task: Status " + forPeriod)
      statusPeriod(lines, forPeriod, outputDir)
     }

     if (tasks.contains("fullDomain")) {
      println("Task: Full Domain " + forPeriod)
      fullDomainPeriod(lines, forPeriod, outputDir)
     }

     if (tasks.contains("domain")) {
      println("Task: Domain " + forPeriod)
      domainPeriod(lines, forPeriod, outputDir)
     }

     if (tasks.contains("domainName")) {
      println("Task: Domain Name " + forPeriod)
      domainNamePeriod(lines, forPeriod, outputDir)
     }

     if (tasks.contains("doi")) {
      println("Task: DOI " + forPeriod)
      doiPeriod(lines, forPeriod, outputDir)
     }

     if (tasks.contains("doiDomain")) {
      println("Task: DOI Domain " + forPeriod)
      doiDomainPeriod(lines, forPeriod, outputDir)
     }

     if (tasks.contains("topDomains")) {
      println("Task: Top Domains " + forPeriod)
      topDomainsPeriod(lines, forPeriod, outputDir)
     }    
  }
}