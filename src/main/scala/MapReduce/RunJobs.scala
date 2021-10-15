package MapReduce

import HelperUtils.CreateLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import Generation.RSGStateMachine.*
import Generation.*
import HelperUtils.Parameters.*

object RunJobs {

  //this is the main starting point for the log generator
  def main(args: Array[String]): Unit = {


    val logger = CreateLogger(classOf[RunJobs.type])

    //  val logFuture = Future {
    //    LogMsgSimulator(init(RandomStringGenerator((Parameters.minStringLength, Parameters.maxStringLength), Parameters.randomSeed)), Parameters.maxCount)
    //  }
    //  Try(Await.result(logFuture, Parameters.runDurationInMinutes)) match {
    //    case Success(value) => logger.info(s"Log data generation has completed after generating ${Parameters.maxCount} records.")
    //    case Failure(exception) => logger.info(s"Log data generation has completed within the allocated time, ${Parameters.runDurationInMinutes}")
    //  }

    logger.info("----------------------------------Starting Job 1------------------------------------------------")
    // Job 1: Distribution of diff type of msg across predefined time interval and
    // injected string instances of the designated regex pattern for these log message type
    val job1 = Job.getInstance(new Configuration(), "basic")
    job1.setJarByClass(this.getClass)
    //Setting mapper
    job1.setMapperClass(classOf[Mappers])
    job1.setCombinerClass(classOf[Reducers])

    //Setting reducer
    job1.setReducerClass(classOf[Reducers])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, new Path(args(1) + "/Job1"))
    job1.waitForCompletion(true)

//    logger.info("----------------------------------Starting Job 2------------------------------------------------")
//    // Job 2: Compute time intervals sorted in the descending order that contained most log messages of the type ERROR
//    // with injected regex pattern string instances.
//    val job2 = Job.getInstance(new Configuration(), "basic2")
//    job2.setJarByClass(this.getClass)
//    //Setting mapper
//    job2.setMapperClass(classOf[MappersJob2])
//    job2.setCombinerClass(classOf[ReducersJob2])
//
//    //Setting reducer
//    job2.setReducerClass(classOf[ReducersJob2])
//    job2.setOutputKeyClass(classOf[Text])
//    job2.setOutputValueClass(classOf[IntWritable])
//
//    FileInputFormat.addInputPath(job2, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job2, new Path(args(1) + "/Job2"))
//    job2.waitForCompletion(true)
//
//
//    logger.info("----------------------------------Starting Job 3------------------------------------------------")
//    // Job 1: Distribution of diff type of msg across predefined time interval and
//    // injected string instances of the designated regex pattern for these log message type
//    val job3 = Job.getInstance(new Configuration(), "basic3")
//    job3.setJarByClass(this.getClass)
//    //Setting mapper
//    job3.setMapperClass(classOf[MappersJob3])
//    job3.setCombinerClass(classOf[ReducersJob3])
//
//    //Setting reducer
//    job3.setReducerClass(classOf[ReducersJob3])
//    job3.setOutputKeyClass(classOf[Text])
//    job3.setOutputValueClass(classOf[IntWritable])
//
//    FileInputFormat.addInputPath(job3, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job3, new Path(args(1) + "/Job3"))
//    job3.waitForCompletion(true)
  }
}
