package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import scala.io.Source

class MappersJob3 extends Mapper[LongWritable, Text, Text, IntWritable] {

  //Logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  //Fetch regex pattern
  val pattern = HelperUtils.Parameters.generatingPattern.r

  //Assign value for Map
  val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Starting Mapper Execution")
    //Read file and convert to String List
    val fileContent = value.toString.split("\n").toList
    //Loop through each list item
    fileContent.foreach(line => {
      //Check if Pattern exists in the log entry
      if (pattern.findFirstIn(line) != None) {
        //Split log entry between actual log message and other text in the log entry
        val logEntry = line.split(" - ").map(_.trim)
        //Split the other text in the log entry to get type of log message
        val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
        //Fetch log message type [DEBUG, ERROR, INFO, WARN]
        val logMsgType = logParser(2)
        context.write(new Text(logMsgType), one)
      }
    })
    logger.info("Mapper Execution Completed")
  }
}
