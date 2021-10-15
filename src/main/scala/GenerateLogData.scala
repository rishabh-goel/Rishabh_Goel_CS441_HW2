/*
 *
 *  Copyright (c) 2021. Mark Grechanik and Lone Star Consulting, Inc. All rights reserved.
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under
 *   the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 *
 */
import Generation.{LogMsgSimulator, RandomStringGenerator}
import HelperUtils.{CreateLogger, Parameters}
import MapReduce.{Mappers, MappersJob2, MappersJob3, Reducers, ReducersJob2, ReducersJob3}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters.*
import scala.concurrent.{Await, Future, duration}
import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object GenerateLogData:
  val logger = CreateLogger(classOf[GenerateLogData.type])

//this is the main starting point for the log generator
def main(args: Array[String]): Unit = {
  import Generation.RSGStateMachine.*
  import Generation.*
  import HelperUtils.Parameters.*
  import GenerateLogData.*

  logger.info("Log data generator started...")
  val INITSTRING = "Starting the string generation"
  val init = unit(INITSTRING)

//  val logFuture = Future {
//    LogMsgSimulator(init(RandomStringGenerator((Parameters.minStringLength, Parameters.maxStringLength), Parameters.randomSeed)), Parameters.maxCount)
//  }
//  Try(Await.result(logFuture, Parameters.runDurationInMinutes)) match {
//    case Success(value) => logger.info(s"Log data generation has completed after generating ${Parameters.maxCount} records.")
//    case Failure(exception) => logger.info(s"Log data generation has completed within the allocated time, ${Parameters.runDurationInMinutes}")
//  }

//  logger.info("----------------------------------Starting Job 1------------------------------------------------")
//  // Job 1: Distribution of diff type of msg across predefined time interval and
//  // injected string instances of the designated regex pattern for these log message type
//  val job1 = Job.getInstance(new Configuration(), "basic")
//  job1.setJarByClass(this.getClass)
//  //Setting mapper
//  job1.setMapperClass(classOf[Mappers])
//  job1.setCombinerClass(classOf[Reducers])
//
//  //Setting reducer
//  job1.setReducerClass(classOf[Reducers])
//  job1.setOutputKeyClass(classOf[Text])
//  job1.setOutputValueClass(classOf[IntWritable])
//
//  FileInputFormat.addInputPath(job1, new Path(args(0)))
//  FileOutputFormat.setOutputPath(job1, new Path(args(1) + "/Job1"))
//  job1.waitForCompletion(true)
//
//  logger.info("----------------------------------Starting Job 2------------------------------------------------")
//  // Job 2: Compute time intervals sorted in the descending order that contained most log messages of the type ERROR
//  // with injected regex pattern string instances.
//  val job2 = Job.getInstance(new Configuration(), "basic2")
//  job2.setJarByClass(this.getClass)
//  //Setting mapper
//  job2.setMapperClass(classOf[MappersJob2])
//  job2.setCombinerClass(classOf[ReducersJob2])
//
//  //Setting reducer
//  job2.setReducerClass(classOf[ReducersJob2])
//  job2.setOutputKeyClass(classOf[Text])
//  job2.setOutputValueClass(classOf[IntWritable])
//
//  FileInputFormat.addInputPath(job2, new Path(args(0)))
//  FileOutputFormat.setOutputPath(job2, new Path(args(1) + "/Job2"))
//  job2.waitForCompletion(true)
//
//
//  logger.info("----------------------------------Starting Job 3------------------------------------------------")
//  // Job 1: Distribution of diff type of msg across predefined time interval and
//  // injected string instances of the designated regex pattern for these log message type
//  val job3 = Job.getInstance(new Configuration(), "basic3")
//  job3.setJarByClass(this.getClass)
//  //Setting mapper
//  job3.setMapperClass(classOf[MappersJob3])
//  job3.setCombinerClass(classOf[ReducersJob3])
//
//  //Setting reducer
//  job3.setReducerClass(classOf[ReducersJob3])
//  job3.setOutputKeyClass(classOf[Text])
//  job3.setOutputValueClass(classOf[IntWritable])
//
//  FileInputFormat.addInputPath(job3, new Path(args(0)))
//  FileOutputFormat.setOutputPath(job3, new Path(args(1) + "/Job3"))
//  job3.waitForCompletion(true)
}
  

