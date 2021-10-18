package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.*
import java.lang

class ReducersJob2_2 extends Reducer[Text, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val map2 = scala.collection.mutable.Map[Text, Int]()

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    logger.info("Executing reducer to display total count of input from mapper completed")
    //Calculate the sum of each value for a key
    val value = values.asScala.foldLeft("")(_ + _)
    logger.info("------"+new Text(value)+"->"+new Text(key))
    context.write(new Text(value), new Text(key))
    logger.info("Reducer execution completed")
  }
}
