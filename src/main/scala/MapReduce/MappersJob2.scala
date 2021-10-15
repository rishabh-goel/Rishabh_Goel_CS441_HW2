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

class MappersJob2 extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val hdfs = FileSystem.get(new URI("hdfs://localhost:8020/"), new Configuration())
  val path = new Path("/input/LogFileGenerator.2021-10-06.log")
  val stream = hdfs.open(path)

  def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

  val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val pattern = HelperUtils.Parameters.generatingPattern.r
    readLines.takeWhile(_ != null).foreach(line => {
      if (pattern.findFirstIn(line) != None) {
        val s = line.split(" - ").map(_.trim)
        val s1 = s(0).replace("  ", " ").split(" ").map(_.trim)
        if(s1(2).equals("ERROR"))
        {
          val time = s1(0).split(':').map(_.trim)
          val hh_min = time(0) +":"+ time(1)
          context.write(new Text(hh_min), one)
        }
      }
    })
  }
}
