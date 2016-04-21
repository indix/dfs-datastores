package com.indix.pail

import java.io.IOException
import java.util

import com.backtype.hadoop.pail.SequenceFileFormat.SequenceFilePailInputFormat
import com.backtype.hadoop.pail.{PailOutputFormat, PailRecordInfo, PailStructure}
import com.backtype.support.Utils
import com.indix.pail.PailMigrate._
import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.log4j.Logger

class PailMigrate extends Tool {
  val logger = Logger.getLogger(this.getClass)

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

  override def run(arguments: Array[String]): Int = {

    val args = Args(arguments)

    val inputDir = args("input-dir")

    val outputDir = args("output-dir")

    val targetSpecClass = args("target-pail-spec")

    val recordType = args("record-type")
    val recordClass = Class.forName(recordType)

    keepSourceFiles = args.boolean("keep-source")

    val targetPailStructure = Class.forName(targetSpecClass).newInstance().asInstanceOf[PailStructure[recordClass.type]]

    val jobConf = new JobConf(getConf)
    jobConf.setJobName("Pail Migration job (from one scheme to another)")

    jobConf.setInputFormat(classOf[SequenceFilePailInputFormat])
    FileInputFormat.addInputPath(jobConf, new Path(inputDir))

    jobConf.setMapOutputKeyClass(classOf[Text])
    jobConf.setMapOutputValueClass(classOf[BytesWritable])

    jobConf.setOutputFormat(classOf[PailOutputFormat])
    FileOutputFormat.setOutputPath(jobConf, new Path(outputDir))

    Utils.setObject(jobConf, PailMigrate.OUTPUT_STRUCTURE, targetPailStructure)

    jobConf.setMapperClass(classOf[PailMigrateMapper])
    jobConf.setReducerClass(classOf[PailMigrateReducer])

    jobConf.setNumReduceTasks(200)
    jobConf.setJarByClass(this.getClass)

    val job = new JobClient(jobConf).submitJob(jobConf)

    logger.info(s"Pail Migrate triggered for $inputDir")
    logger.info("Submitted job " + job.getID)

    while (!job.isComplete) {
      Thread.sleep(30 * 1000)
    }

    if (!job.isSuccessful) throw new IOException("Pail Migrate failed")

    0 // return success, failures throw an exception anyway!
  }

  def getConf: Configuration = configuration

  def setConf(config: Configuration): Unit = { configuration = config }
}

object PailMigrate {
  val OUTPUT_STRUCTURE = "pail.migrate.output.structure"

  var configuration: Configuration = null
  var keepSourceFiles: Boolean = true

  class PailMigrateMapper extends Mapper[PailRecordInfo, BytesWritable, Text, BytesWritable] {
    var outputPailStructure: PailStructure[Any] = null
    val logger = Logger.getLogger(this.getClass)

    override def map(key: PailRecordInfo, value: BytesWritable,
                     outputCollector: OutputCollector[Text, BytesWritable],
                     reporter: Reporter): Unit = {
      val record = outputPailStructure.deserialize(value.getBytes)
      val key = new Text(Utils.join(outputPailStructure.getTarget(record), "/"))
      val inputFileLocations = reporter.getInputSplit.getLocations
      if (!keepSourceFiles) {
        inputFileLocations.foreach{
        p => val path = new Path(p)
          val fs = path.getFileSystem(configuration)
            logger.info(s"Deleting path $p")
            val deleteStatus = fs.delete(path, true)

            if (!deleteStatus)
              logger.warn(s"Deleting $p failed. \n *** Please delete the source manually ***")
            else
              logger.info(s"Deleting $p completed successfully.")
          }
      }
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
      while(iterator.hasNext)
        outputCollector.collect(key, iterator.next())
    }
  }

}

object PailMigrateUtil {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration(), new PailMigrate, args)
  }
}


