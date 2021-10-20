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

    val configuration = new Configuration
    //Change separator from tab to comma
    configuration.set("mapred.textoutputformat.separator", ",")

    val logger = CreateLogger(classOf[RunJobs.type])

    logger.info("----------------------------------Starting Job 1------------------------------------------------")
    // Job 1: Distribution of different type of log message type across predefined time interval and
    // injected string instances of the designated regex pattern for these log message type
    val job1 = Job.getInstance(configuration, "Job1")
    job1.setJarByClass(this.getClass)

    //Setting mapper
    job1.setMapperClass(classOf[MappersJob1])
    //Setting mapper
    job1.setCombinerClass(classOf[ReducersJob1])
    //Setting reducer
    job1.setReducerClass(classOf[ReducersJob1])

    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, new Path(args(1) + "/Job1"))
    job1.waitForCompletion(true)


    logger.info("----------------------------------Starting Job 2------------------------------------------------")
    // Job 2: Compute time intervals sorted in the descending order that contained most log messages of the type ERROR
    // with injected regex pattern string instances.
    val job2 = Job.getInstance(configuration, "Job2")
    job2.setJarByClass(this.getClass)

    //Setting mapper
    job2.setMapperClass(classOf[MappersJob2])
    //Setting mapper output key and value class
    job2.setMapOutputKeyClass(classOf[Text])
    job2.setMapOutputValueClass(classOf[Text])

    //Setting reducer
    job2.setReducerClass(classOf[ReducersJob2])
    //Setting output key and value class
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job2, new Path(args(0)))
    //Store the output of ReducersJob2 which will be input for MappersJob2_Chain
    FileOutputFormat.setOutputPath(job2, new Path(args(1) + "/Job2_IntermediateResult"))
    job2.waitForCompletion(true)

    logger.info("----------------------------------Starting Job 2.2------------------------------------------------")
    val job2_2 = Job.getInstance(configuration, "Job2_Chain")
    job2_2.setJarByClass(this.getClass)

    //Setting mapper
    job2_2.setMapperClass(classOf[MappersJob2_Chain])
    //Setting Comparator Class to sort in descending order
    job2_2.setSortComparatorClass(classOf[ValueComparator])
    //Setting output key and value class
    job2_2.setMapOutputKeyClass(classOf[Text])
    job2_2.setMapOutputValueClass(classOf[Text])

    //Setting reducer
    job2_2.setReducerClass(classOf[ReducersJob2_Chain])
    //Setting output key and value class
    job2_2.setOutputKeyClass(classOf[Text])
    job2_2.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job2_2, new Path(args(1) + "/Job2_IntermediateResult"))
    FileOutputFormat.setOutputPath(job2_2, new Path(args(1) + "/Job2"))
    job2_2.waitForCompletion(true)

    logger.info("----------------------------------Starting Job 3------------------------------------------------")
    // Job 3: Distribution of different type of log message across the entire log file
    val job3 = Job.getInstance(configuration, "Job3")
    job3.setJarByClass(this.getClass)

    //Setting mapper
    job3.setMapperClass(classOf[MappersJob3])
    //Setting Combiner
    job3.setCombinerClass(classOf[ReducersJob3])

    //Setting reducer
    job3.setReducerClass(classOf[ReducersJob3])
    //Setting output key and value class
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job3, new Path(args(0)))
    FileOutputFormat.setOutputPath(job3, new Path(args(1) + "/Job3"))
    job3.waitForCompletion(true)


    logger.info("----------------------------------Starting Job 4------------------------------------------------")
    // Job 4: Compute number of characters in each log message for each log message type that contain the highest
    // number of characters in the detected instances of the designated regex pattern
    val job4 = Job.getInstance(configuration, "Job4")
    job4.setJarByClass(this.getClass)

    //Setting mapper
    job4.setMapperClass(classOf[MappersJob4])
    //Setting Combiner
    job4.setCombinerClass(classOf[ReducersJob4])

    //Setting reducer
    job4.setReducerClass(classOf[ReducersJob4])
    //Setting output key and value class
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job4, new Path(args(0)))
    FileOutputFormat.setOutputPath(job4, new Path(args(1) + "/Job4"))
    job4.waitForCompletion(true)

  }
}
