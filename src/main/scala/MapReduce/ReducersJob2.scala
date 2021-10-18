package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConverters.*
import scala.collection.immutable.ListMap

class ReducersJob2 extends Reducer[Text, Text, Text, Text] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val map2 = scala.collection.mutable.Map[Text, IntWritable]()

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    logger.info("Executing reducer to display total count of input from mapper completed")
    //Calculate the sum of each value for a key
    val sum = values.asScala.size
    context.write(new Text(key), new Text(sum.toString))
    logger.info("Reducer execution completed")
  }
}
