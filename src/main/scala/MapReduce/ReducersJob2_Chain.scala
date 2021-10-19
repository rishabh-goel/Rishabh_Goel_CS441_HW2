package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.*
import java.lang

class ReducersJob2_Chain extends Reducer[Text, Text, Text, Text]{
  //Logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    logger.info("Executing reducer to display total count of input from mapper")

    //Creates a concatenation of values in the list
    val value = values.asScala.foldLeft("")(_ + _)

    //Again swap the values
    //Output of Previous Mapper eg. 123 -> 15:00, 245 -> 15:30, 100 -> 16:00
    //Output of Current Reducer eg. 15:30 -> 245, 15:00 -> 123,  16:00 -> 100
    context.write(new Text(value), new Text(key))
    logger.info("Reducer execution completed")
  }
}
