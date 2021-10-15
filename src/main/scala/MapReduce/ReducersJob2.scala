package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConverters.*
import scala.collection.immutable.ListMap

class ReducersJob2 extends Reducer[Text, IntWritable, Text, IntWritable] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val map2 = scala.collection.mutable.Map[Text, Int]()

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Executing reducer to display total count of input from mapper completed")
    //Calculate the sum of each value for a key
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    map2 += (key -> sum)

    val listMap = ListMap(map2.toSeq.sortWith(_._2 > _._2):_*)

    listMap.foreach(x => context.write(new Text(x._1), new IntWritable(x._2)))
    logger.info("Reducer execution completed")
  }
}
