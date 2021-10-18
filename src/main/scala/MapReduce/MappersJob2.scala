package MapReduce

import HelperUtils.{CreateLogger, Parameters}

import scala.io.Source
import HelperUtils.Parameters.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.util.regex.Pattern
import scala.util.{Failure, Success, Try}

class MappersJob2 extends Mapper[Object, Text, Text, Text] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  val timeInterval = HelperUtils.Parameters.timeInterval
  
  val one = new Text("1")

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    val pattern = HelperUtils.Parameters.generatingPattern.r
    val fileContent = value.toString.split("\n").toList
    fileContent.foreach(line => {
      if (pattern.findFirstIn(line) != None) {
        val logEntry = line.split(" - ").map(_.trim)
        val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
        if(logParser(2).equals("ERROR"))
        {
          val logTime = logParser(0).split(':').map(_.trim)
          val logHour = logTime(0).toInt
          val logMin = logTime(1).toInt
          if(logMin < 10)
          {
            val timeKey5 = logHour.toString.concat(":05")
            context.write(new Text(timeKey5), one)
          }
          else if (logMin < 15)
          {
            val timeKey10 = logHour.toString.concat(":10")
            context.write(new Text(timeKey10), one)
          }
          else if (logMin < 20)
          {
            val timeKey10 = logHour.toString.concat(":15")
            context.write(new Text(timeKey10), one)
          }
          else if (logMin < 25)
          {
            val timeKey10 = logHour.toString.concat(":20")
            context.write(new Text(timeKey10), one)
          }
//          if(logMin < 30)
//          {
//            val timeKey30 = logHour.toString.concat(":00")
//            context.write(new Text(timeKey30), one)
//          }
//          else
//          {
//            val timeKey60 = logHour.toString.concat(":30")
//            context.write(new Text(timeKey60), one)
//          }
        }
      }
    })
  }
}
