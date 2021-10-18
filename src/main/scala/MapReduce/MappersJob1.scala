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


class MappersJob1 extends Mapper[LongWritable, Text, Text, IntWritable] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  val pattern = HelperUtils.Parameters.generatingPattern.r

  val r = new scala.util.Random

  val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val fileContent = value.toString.split("\n").toList
    val first_entry = fileContent.head
    val hrs_start = first_entry.take(2).toInt
    val mins_start = first_entry.substring(3, 5).toInt
    val mins_end = mins_start + HelperUtils.Parameters.timeInterval

    fileContent.foreach(line => {
      if (pattern.findFirstIn(line) != None) {
        val logEntry = line.split(" - ").map(_.trim)
        val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
        val logTime = logParser(0).split(':').map(_.trim)
        val logHour = logTime(0).toInt
        val logMin = logTime(1).toInt
        val logMsgType = logParser(2)

        if((hrs_start == logHour) && (mins_start <= logMin) && (logMin <= mins_end))
          context.write(new Text(logMsgType), one)
      }
    })
  }
}