package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import scala.io.Source

class MappersJob3 extends Mapper[LongWritable, Text, Text, IntWritable] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  val pattern = HelperUtils.Parameters.generatingPattern.r
  val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val fileContent = value.toString.split("\n").toList
    fileContent.foreach(line => {
      if (pattern.findFirstIn(line) != None) {
        val logEntry = line.split(" - ").map(_.trim)
        val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
        val logMsgType = logParser(2)
        context.write(new Text(logMsgType), one)
      }
    })
  }
}
