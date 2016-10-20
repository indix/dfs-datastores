package com.indix.pail

import com.backtype.hadoop.pail.Pail
import com.indix.commons.FSUtils
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import util.{DateHelper, DateTimeFormatter}


class PailConsolidate(inputDir: String, subDir: String, pipelineLabel: String, nonMRConsolidation: Boolean = false)  {
  val logger = LoggerFactory.getLogger(this.getClass)

  def conf: Configuration = new Configuration()

  val component = "CONSOLIDATE_" + pipelineLabel
  val writeLock = new PailLock(PailLockType.WRITE, component, inputDir)

  def consolidateUsingPail(fileSystem: FileSystem, consolidationDir: String) {
    val pail = Pail.create(fileSystem, consolidationDir, false)
    pail.consolidate()
  }

  def consolidateUsingPailNonMR(fileSystem: FileSystem, consolidationDir: String): Unit = {
    val pail = Pail.create(fileSystem, consolidationDir, false)
    pail.consolidateNonMR()
  }

  def run() = {
    try {
      logger.info("Starting consolidate... " + subDir + " Aquiring write lock on " + inputDir)
      val fileSystem = new Path(subDir).getFileSystem(conf)
      writeLock.acquire()
      if(nonMRConsolidation) consolidateUsingPailNonMR(fileSystem, subDir) else consolidateUsingPail(fileSystem, subDir)
      logger.info("Consolidate done.")
    } finally {
      writeLock.release()
    }
  }
}

object PailConsolidate {
  def main(args: Array[String]) = {
    if (args.length < 1) {
      println("Usage: java -cp <jar> PailConsolidate /root/dir/to/consolidate [singleNodeConsolidation: true or false]")
      System.exit(1)
    }

    val isSingleNodeConsolidation = if(args.length == 2) args(1).toBoolean else false
    val pailConsolidate = new PailConsolidate(args(0), args(0), Option(System.getenv("GO_PIPELINE_LABEL")).filter(_.nonEmpty).getOrElse("MANUAL"), isSingleNodeConsolidation)
    pailConsolidate.run()

  }
}

object IxPailConsolidator extends FSUtils with ArgsParser {
  def conf: Configuration = new Configuration()

  def main(args: Array[String]) = {
    implicit val cli = new PosixParser().parse(options, args)
    val pailRoot = cmdArgs("input-dir")
    val pipelineFromEnv = Option(System.getenv("GO_PIPELINE_LABEL")).filter(_.nonEmpty).getOrElse("MANUAL")
    val pipelineLabel = cmdOptionalArgs("pipeline").getOrElse(pipelineFromEnv)
    val numTimePartitionUnitsToCover = cmdOptionalArgs("num-partition-units").getOrElse("2").toInt
    val strategy = cmdOptionalArgs("strategy").getOrElse("all")
    val thisMoment = DateTime.now()

    def getSubDirToProcess(strategy: String, thisMoment: DateTime, i: Int) = strategy match {
      case "hourly" => thisMoment.minusHours(i).toString(DateTimeFormatter.format())
      case "daily" => thisMoment.minusDays(i).toString(DateTimeFormatter.format())
      case "weekly" => DateHelper.weekInterval(thisMoment.minusWeeks(i))
      case "all" => ""
      case _ => throw new RuntimeException("Unsupported strategy. Supported ones are: hourly|daily|weekly|all")
    }

    val dirsToConsolidate = (0 until numTimePartitionUnitsToCover).map { i =>
      pailRoot + "/" + getSubDirToProcess(strategy, thisMoment, i)
    }.filter(exists).toSet

    dirsToConsolidate.foreach { subDirToProcess => new PailConsolidate(pailRoot, subDirToProcess, pipelineLabel).run() }
  }

  override val options = {
    val cmdOptions = new Options()
    cmdOptions.addOption("i", "input-dir", true, "Input Directory")
    cmdOptions.addOption("p", "pipeline", true, "Pipeline")
    cmdOptions.addOption("n", "num-parittion-units", true, "Number of Partition Units")
    cmdOptions.addOption("s", "strategy", true, "Pail Strategy")
    cmdOptions
  }


}
