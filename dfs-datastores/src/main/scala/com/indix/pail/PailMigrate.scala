package com.indix.pail

import java.io.{File, IOException}
import java.util

import _root_.util.DateHelper
import com.backtype.hadoop.pail.SequenceFileFormat.SequenceFilePailInputFormat
import com.backtype.hadoop.pail._
import com.backtype.support.Utils
import com.indix.pail.PailMigrate._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.log4j.Logger
import org.joda.time.DateTime

import scala.collection.JavaConversions._

class BlacklistedPailPathLister extends PailPathLister {
  val delegate = new AllPailPathLister()
  var blacklistedPaths: Array[String] = _

  def initConf(blacklistedPaths: Array[String]): Unit = {
    this.blacklistedPaths = blacklistedPaths
  }


  override def getPaths(p: Pail[_]): util.List[Path] = {
    val paths = delegate.getPaths(p)
    paths.filter(p => !blacklistedPaths.exists(excludePattern => p.toUri.toString.startsWith(excludePattern)))
  }
}

class PailMigrate extends Tool with ArgsParser {
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

    implicit val cli = new PosixParser().parse(options, args)

    val inputDir = cmdArgs("input-dir")

    val outputDir = cmdArgs("output-dir")

    val targetSpecClass = cmdArgs("target-pail-spec")

    val recordType = cmdArgs("record-type")
    val recordClass = Class.forName(recordType)

    val keepSourceFiles = cmdOptionalArgs("keep-source").getOrElse("false").toBoolean

    val runReducer = cmdOptionalArgs("run-reducer").getOrElse("true").toBoolean
    val blacklistedPath = cmdOptionalArgs("blacklist").getOrElse("")

    val targetPailStructure = Class.forName(targetSpecClass).newInstance().asInstanceOf[PailStructure[recordClass.type]]

    val jobConf = new JobConf(getConf)
    // FIXME Make pool and priority configurable
    jobConf.setJobName("Pail Migration job (from one scheme to another)")
    jobConf.set("mapred.fairscheduler.pool", "hadoop")
    jobConf.setJobPriority(JobPriority.VERY_HIGH)

    val path: Path = new Path(inputDir)
    val fs = path.getFileSystem(getConf)

    if(!fs.exists(path)) {
      logger.warn("Input directory is not valid/found. Could be migrated or due to a invalid path")
      return 0
    }

    jobConf.setInputFormat(classOf[SequenceFilePailInputFormat])
    FileInputFormat.addInputPath(jobConf, new Path(inputDir))

    jobConf.setMapOutputKeyClass(classOf[Text])
    jobConf.setMapOutputValueClass(classOf[BytesWritable])

    jobConf.setOutputFormat(classOf[PailOutputFormat])
    FileOutputFormat.setOutputPath(jobConf, new Path(outputDir))

    Utils.setObject(jobConf, PailMigrate.OUTPUT_STRUCTURE, targetPailStructure)


    if (blacklistedPath.nonEmpty) {
      val blacklistedPaths = io.Source.fromFile(new File(blacklistedPath))("UTF-8").getLines().toArray
      val pathLister = new BlacklistedPailPathLister()
      pathLister.initConf(blacklistedPaths)

      PailFormatFactory.setPailPathLister(jobConf, pathLister)
    }

    jobConf.setMapperClass(classOf[PailMigrateMapper])
    jobConf.setJarByClass(this.getClass)

    if (runReducer) {
      jobConf.setReducerClass(classOf[PailMigrateReducer])
      jobConf.setNumReduceTasks(200)
    } else {
      jobConf.setNumReduceTasks(0)
    }

    val job = new JobClient(jobConf).submitJob(jobConf)

    logger.info(s"Pail Migrate triggered for $inputDir")
    logger.info("Submitted job " + job.getID)

    while (!job.isComplete) {
      Thread.sleep(30 * 1000)
    }

    if (!job.isSuccessful) throw new IOException("Pail Migrate failed")

    if (!keepSourceFiles) {
      logger.info(s"Deleting path $inputDir")
      val deleteStatus = fs.delete(path, true)

      if (!deleteStatus)
        logger.warn(s"Deleting $inputDir failed. \n *** Please delete the source manually ***")
      else
        logger.info(s"Deleting $inputDir completed successfully.")
    }

    0 // return success, failures throw an exception anyway!
  }

  override def getConf: Configuration = configuration

  override def setConf(configuration: Configuration): Unit = this.configuration = configuration

  override val options = {
    val cmdOptions = new Options()
    cmdOptions.addOption("i", "input-dir", true, "Input Directory")
    cmdOptions.addOption("o", "output-dir", true, "Output Directory")
    cmdOptions.addOption("t", "target-pail-spec", true, "Target Pail Spec")
    cmdOptions.addOption("r", "record-type", true, "Record Type")
    cmdOptions.addOption("k", "keep-source", false, "Keep Source")
    cmdOptions.addOption("g", "run-reducer", false, "Run Reducer")
    cmdOptions.addOption("b", "blacklist", false, "Blacklist Path")
    cmdOptions
  }
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

  class PailMigrateReducer extends Reducer[Text, BytesWritable, Text, BytesWritable] {

    override def close(): Unit = {}

    override def configure(jobConf: JobConf): Unit = {}

    override def reduce(key: Text, iterator: util.Iterator[BytesWritable], outputCollector: OutputCollector[Text, BytesWritable], reporter: Reporter): Unit = {
      while (iterator.hasNext)
        outputCollector.collect(key, iterator.next())
    }
  }

}

object PailMigrateUtil {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration(), new PailMigrate, args)
  }
}

object IxPailArchiver extends ArgsParser {
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    val lastWeekBucket = DateHelper.weekInterval(new DateTime(System.currentTimeMillis()).minusDays(14))
    implicit val cli = new PosixParser().parse(options, args)
    val baseInputDir = cmdArgs("base-input-dir")

    val inputDirPath: Path = new Path(baseInputDir, lastWeekBucket)
    val configuration = new Configuration()
    val fs = inputDirPath.getFileSystem(configuration)

    if (fs.exists(inputDirPath)) {
      val newParams = args ++ Array("--input-dir", inputDirPath.toString)
      ToolRunner.run(configuration, new PailMigrate, newParams)
    } else {
      logger.info("The following location doesn't exist:" + inputDirPath)
    }

  }

  override val options: Options = {
    val cmdOptions = new Options()
    cmdOptions.addOption("i", "base-input-dir", true, "Input Directory")
    cmdOptions
  }
}

