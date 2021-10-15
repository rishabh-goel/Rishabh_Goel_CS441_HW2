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
  val hdfs = FileSystem.get(new URI("hdfs://localhost:8020/"), new Configuration())
  val path = new Path("/input/LogFileGenerator.2021-10-06.log")
  val stream = hdfs.open(path)
  val pattern = HelperUtils.Parameters.generatingPattern.r

  val r = new scala.util.Random
  val hrs_start = 21+r.nextInt(1)
  val mins_start = r.nextInt(45)
  val sec_start = r.nextInt(45)
  val mins_end = mins_start + HelperUtils.Parameters.timeInterval
  val time = String.valueOf(hrs_start).concat(":".concat(String.valueOf(mins_start).concat(":".concat(String.valueOf(sec_start)))))

  logger.info("Time is = " + time)

  def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

  val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    readLines.takeWhile(_ != null).foreach(line => {
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