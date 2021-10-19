package MapReduce

import org.apache.hadoop.io.IntWritable.Comparator
import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}

import scala.language.postfixOps
class ValueComparator extends WritableComparator(classOf[Text], true){

  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val key1 = a.toString.toInt
    val key2 = b.toString.toInt

    //Return the value in descending order
    -1*key1.compareTo(key2)
  }
}