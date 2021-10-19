import HelperUtils.{CreateLogger, Parameters}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.runtime.stdLibPatches.Predef.assert

class MRTest extends AnyFlatSpec with Matchers{

  val pattern = HelperUtils.Parameters.generatingPattern.r
  val filename = "log/LogFileGenerator.2021-10-18.log"
  val lines = Source.fromFile(filename).getLines.toList

  //Test 1
  it should "check if config files exists" in {
    val jobConfig = ConfigFactory.load("application")
    assert(!jobConfig.isEmpty)
  }

  //Test 2
  it should "check if pattern exist" in {
    pattern.findFirstIn("Xu3`?sDn.$YVb!mwE$$#N>VP8lce1ag0ae0cf3be2A7fcg3bf3T'HnFt8r9c'v^<^yx4q-x}5)") should not be (null)
  }

  //Test 3
  it should "check if log file is not empty" in {
    lines.size > 0
  }

  //Test 4
  it should "check distinct error messages" in {
    lines.foreach(line => {
      if (pattern.findFirstIn(line) != None) {
        val logEntry = line.split(" - ").map(_.trim)
        val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
        val logMsgType = logParser(2)
        val list = List("DEBUG", "ERROR", "INFO", "WARN")

        list should contain (logMsgType)
      }
    })
  }

  //Test 5
  it should "check if no of ERROR messages is equal to that from the Map Reduce Job3 " in {
    val buf = scala.collection.mutable.ListBuffer.empty[Int]
    lines.foreach(line => {
      val logEntry = line.split(" - ").map(_.trim)
      val logParser = logEntry(0).replace("  ", " ").split(" ").map(_.trim)
      val logMsgType = logParser(2)
      if (logMsgType=="ERROR" && pattern.findFirstIn(line) != None) {
        buf += 1
      }
    })

    buf.size === 26779
  }

}