package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}
import java.lang
import scala.collection.JavaConverters.*

class ReducersJob1 extends Reducer[Text, IntWritable, Text, IntWritable]{

  //Logger
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Executing reducer to display total count of input from mapper")
    //Calculate the sum of each value for a key
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    context.write(key, new IntWritable(sum))
    logger.info("Reducer Execution Completed")
  }
}
