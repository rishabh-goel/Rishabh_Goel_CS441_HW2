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
  //Logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  //Fetch regex pattern
  val pattern = HelperUtils.Parameters.generatingPattern.r

  //Assign value for Map
  val one = new Text("1")

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
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

        //Check if log message is of type ERROR
        if(logParser(2).equals("ERROR"))
        {
          //Split log time to get hours and minutes
          val logTime = logParser(0).split(':').map(_.trim)
          val logHour = logTime(0).toInt
          val logMin = logTime(1).toInt

          //Bucket 1 - log entries from hour:00 to hour:14
          if(logMin < 15)
          {
            val timeKey = logHour.toString.concat(":00")
            context.write(new Text(timeKey), one)
          }
          //Bucket 2 - log entries from hour:15 to hour:29
          else if (logMin < 30)
          {
            val timeKey = logHour.toString.concat(":15")
            context.write(new Text(timeKey), one)
          }
          //Bucket 3 - log entries from hour:30 to hour:44
          else if (logMin < 45)
          {
            val timeKey = logHour.toString.concat(":30")
            context.write(new Text(timeKey), one)
          }
          //Bucket 4 - log entries from hour:45 to hour:59
          else if (logMin <=59)
          {
            val timeKey = logHour.toString.concat(":45")
            context.write(new Text(timeKey), one)
          }
        }
      }
    })

    logger.info("Mapper Execution Completed")
  }
}
