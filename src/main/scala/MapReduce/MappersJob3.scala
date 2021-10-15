package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

class MappersJob3 extends Mapper[LongWritable, Text, Text, IntWritable] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val hdfs = FileSystem.get(new URI("hdfs://localhost:8020/"), new Configuration())
  val path = new Path("/input/LogFileGenerator.2021-10-06.log")
  val stream = hdfs.open(path)
  val pattern = HelperUtils.Parameters.generatingPattern.r

  def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

  val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    readLines.takeWhile(_ != null).foreach(line => {
      if (pattern.findFirstIn(line) != None) {
        val logEntry = line.split(" - ").map(_.trim)
        val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
        val logMsgType = logParser(2)
        context.write(new Text(logMsgType), one)
      }
    })
  }
}
