package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.*
import java.lang
import scala.collection.immutable.ListMap
import scala.math.Ordered.orderingToOrdered

class ReducersJob4 extends Reducer[Text, IntWritable, Text, IntWritable] {
  //Logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Executing reducer to display total count of input from mapper")
    //Calculate the max of each value for a key
    val maxVal = values.asScala.reduceLeft((x,y) => if (x > y) x else y)
    context.write(key, maxVal)
    logger.info("Reducer execution completed")
  }
}
