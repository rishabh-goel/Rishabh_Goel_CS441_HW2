package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

class MappersJob2_2 extends Mapper[Object, Text, Text, Text]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    val text = value.toString.split("\t")
    val k = text(0)
    val v = text(1)
    logger.info(v.toString + "->" + k.toString)
    context.write(new Text(v), new Text(k))
  }
}
