package com.indix.pail

import java.io.IOException

import _root_.util.DateHelper
import com.backtype.hadoop.pail.SequenceFileFormat.SequenceFilePailInputFormat
import com.backtype.hadoop.pail._
import com.backtype.support.Utils
import com.indix.pail.PailMigrate._
import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.log4j.Logger
import org.joda.time.DateTime

import scala.collection.JavaConverters.{asScalaBufferConverter}

class PailMigrate extends Tool {
  val logger = Logger.getLogger(this.getClass)
  var configuration: Configuration = null

  /*
  * Takes an input pail location, an output pail location and a output pail spec
  * - Setup job to process input pail location
  * - Deserialize record
  * - Write to output location using the output spec
  * - If output dir already exists, just append to it, instead of writing to temp and absorbing
  * - Finally, clear out all processed files (disable source removal based on configuration).
  *
  * OutputFormat - PailOutputFormat - needs a PailSpec
  * */

  override def run(args: Array[String]): Int = {

    val cmdArgs = Args(args)

    val inputDir = cmdArgs("input-dir")

    val outputDir = cmdArgs("output-dir")

    val targetSpecClass = cmdArgs("target-pail-spec")

    val recordType = cmdArgs("record-type")
    val recordClass = Class.forName(recordType)

    val keepSourceFiles = cmdArgs("keep-source").toBoolean

    val targetPailStructure = Class.forName(targetSpecClass).newInstance().asInstanceOf[PailStructure[recordClass.type]]

    val jobConf = new JobConf(getConf)

    jobConf.setJobName("Pail Migration job (from one scheme to another)")

    val path: Path = new Path(inputDir)
    val fs = path.getFileSystem(getConf)

    if(!fs.exists(path)) {
      logger.warn("Input directory is not valid/found. Could be migrated or due to a invalid path")
      return 0
    }

    jobConf.setInputFormat(classOf[SequenceFilePailInputFormat])
    val filesAvailableAsOfNow = new Pail(inputDir).getStoredFiles
    filesAvailableAsOfNow.asScala.foreach {inputFileInPail =>
      FileInputFormat.addInputPath(jobConf, inputFileInPail)
    }

    jobConf.setOutputFormat(classOf[PailOutputFormat])
    FileOutputFormat.setOutputPath(jobConf, new Path(outputDir))

    Utils.setObject(jobConf, PailMigrate.OUTPUT_STRUCTURE, targetPailStructure)

    jobConf.setMapperClass(classOf[PailMigrateMapper])
    jobConf.setJarByClass(this.getClass)

    jobConf.setNumReduceTasks(0)

    val job = new JobClient(jobConf).submitJob(jobConf)

    logger.info(s"Pail Migrate triggered for $inputDir")
    logger.info("Submitted job " + job.getID)

    while (!job.isComplete) {
      Thread.sleep(30 * 1000)
    }

    if (!job.isSuccessful) throw new IOException("Pail Migrate failed")

    if (!keepSourceFiles) {
      logger.info(s"Deleting files under $inputDir")
      val deleteStatus = filesAvailableAsOfNow.asScala.forall {pailFile =>
        val status = fs.delete(path, true)
        if(!status) {
          logger.warn(s"Deleting of $pailFile failed.")
        }
        status
      }

      if (!deleteStatus)
        logger.warn(s"Deleting $inputDir failed. \n *** Please delete the source manually ***")
      else
        logger.info(s"Deleting $inputDir completed successfully.")
    }

    0 // return success, failures throw an exception anyway!
  }

  override def getConf: Configuration = configuration

  override def setConf(configuration: Configuration): Unit = this.configuration = configuration
}

object PailMigrate {
  val OUTPUT_STRUCTURE = "pail.migrate.output.structure"

  class PailMigrateMapper extends Mapper[PailRecordInfo, BytesWritable, Text, BytesWritable] {
    var outputPailStructure: PailStructure[Any] = null

    override def map(key: PailRecordInfo, value: BytesWritable, outputCollector: OutputCollector[Text, BytesWritable], reporter: Reporter): Unit = {
      val record = outputPailStructure.deserialize(value.getBytes)
      val key = new Text(Utils.join(outputPailStructure.getTarget(record), "/"))
      outputCollector.collect(key, value)
    }

    override def close(): Unit = {}

    override def configure(jobConf: JobConf): Unit = {
      outputPailStructure = Utils.getObject(jobConf, OUTPUT_STRUCTURE).asInstanceOf[PailStructure[Any]]
    }
  }

}

object PailMigrateUtil {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration(), new PailMigrate, args)
  }
}

object IxPailArchiver {
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val cmdArgs = Args(args)
    val numWeeksToArchive = cmdArgs.optional("num-weeks").getOrElse("1").toInt
    val daysBefore = cmdArgs.optional("days-before").getOrElse("14").toInt
    val baseInputDirs = cmdArgs("base-input-dir")

    val bucketsToMove = 0 until numWeeksToArchive map {
      num => DateHelper.weekInterval(new DateTime(System.currentTimeMillis()).minusDays(daysBefore + (7 * num)))
    }

    baseInputDirs.split(",").foreach{
      baseDir => bucketsToMove.foreach(bucket => migrateToQuarter(args, bucket, baseDir.trim()))
    }

  }

  private def migrateToQuarter(args: Array[String], lastWeekBucket: String, baseInputDir: String) = {
    val inputDirPath: Path = new Path(baseInputDir, lastWeekBucket)
    val configuration = new Configuration()
    val fs = inputDirPath.getFileSystem(configuration)

    if (fs.exists(inputDirPath)) {
      val newArgs = args.filterNot{arg => Array("num-weeks", "days-before").contains(arg)}
      val loc = newArgs.indexOf("--base-input-dir")
      val finalParams: Array[String] = newArgs.slice(0, loc) ++ newArgs.slice(loc + 2, newArgs.length) ++ Array("--input-dir", inputDirPath.toString)
      logger.info(finalParams.mkString(" "))
      ToolRunner.run(configuration, new PailMigrate, finalParams)
    } else {
      logger.info("The following location doesn't exist:" + inputDirPath.getName)
    }
  }
}

