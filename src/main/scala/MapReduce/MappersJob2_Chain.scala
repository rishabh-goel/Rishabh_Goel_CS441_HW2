package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

class MappersJob2_Chain extends Mapper[Object, Text, Text, Text]{

  //Logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    logger.info("Starting Mapper Execution")

    //Read key,value output of ReducerJob2 and parse it as input here
    val text = value.toString.split("\t")
    val k = text(0)   // key(time)
    val v = text(1)   // value(count)

    //swapping key and value
    //Output of Previous Reducer eg. 15:00 -> 123, 15:30 -> 245, 16:00 -> 100
    //Output of Current Mapper   eg. 123 -> 15:00, 245 -> 15:30, 100 -> 16:00
    context.write(new Text(v), new Text(k))

    logger.info("Mapper Execution Completed")
  }
}
