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

  //Logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  //Fetch regex pattern
  val pattern = HelperUtils.Parameters.generatingPattern.r

  //Assign value for Map
  val one = new IntWritable(1)

  //Generate random start time for the analysis
  val r = new scala.util.Random(HelperUtils.Parameters.randomSeed)
  val hrs_start = 20+r.nextInt(2)
  val mins_start = r.nextInt(45)

  logger.info("Starting Time is ----> " + hrs_start+":"+mins_start)

  //Fetch time interval across which MapReduce has to run
  val mins_end = mins_start + HelperUtils.Parameters.timeInterval
  logger.info("Ending Time is ----> " + hrs_start+":"+mins_end)


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
        //Split the other text in the log entry to get log time and type of log message
        val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
        //Split log time to get hours and minutes
        val logTime = logParser(0).split(':').map(_.trim)
        val logHour = logTime(0).toInt
        val logMin = logTime(1).toInt

        //Fetch log message type [DEBUG, ERROR, INFO, WARN]
        val logMsgType = logParser(2)

        //If the time of log entry is between our time interval, add it's log message type to the mapper
        if((hrs_start == logHour) && (mins_start <= logMin) && (logMin <= mins_end))
          context.write(new Text(logMsgType), one)
      }
    })

    logger.info("Mapper Execution Completed")
  }
}