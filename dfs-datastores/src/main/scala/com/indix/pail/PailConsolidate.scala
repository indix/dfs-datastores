package com.indix.pail

import com.backtype.hadoop.pail.Pail
import com.indix.commons.FSUtils
import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import util.{DateTimeFormatter, DateHelper}

class PailConsolidate(inputDir: String, subDir: String, pipelineLabel: String, singleNodeConsolidation: Boolean = false)  {
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
      if(singleNodeConsolidation) consolidateUsingPailNonMR(fileSystem, subDir) else consolidateUsingPail(fileSystem, subDir)
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

object IxPailConsolidator extends FSUtils {
  def conf: Configuration = new Configuration()

  def main(argumentArr: Array[String]) = {
    val args = Args(argumentArr)
    val pailRoot = args("input-dir").stripSuffix("/")
    val pipelineFromEnv = Option(System.getenv("GO_PIPELINE_LABEL")).filter(_.nonEmpty).getOrElse("MANUAL")
    val pipelineLabel = args.getOrElse("pipeline", pipelineFromEnv)
    val numTimePartitionUnitsToCover = args.getOrElse("num-partition-units", "2").toInt
    val strategy = args.getOrElse("strategy", "all")
    val thisMoment = DateTime.now()

    def getSubDirToProcess(strategy: String, thisMoment: DateTime, i: Int) = strategy match {
      case "hourly" => thisMoment.minusHours(i).toString(DateTimeFormatter.format())
      case "daily" => thisMoment.minusDays(i).toString(DateTimeFormatter.format())
      case "weekly" => DateHelper.weekInterval(thisMoment.minusWeeks(i))
      case "all" => ""
      case _ => throw new RuntimeException("Unsupported strategy. Supported ones are: hourly|daily|weekly|all")
    }

    val dirsToConsolidate = (0 to numTimePartitionUnitsToCover - 1).map { i =>
      pailRoot + "/" + getSubDirToProcess(strategy, thisMoment, i)
    }.filter(exists).toSet

    dirsToConsolidate.foreach { subDirToProcess => new PailConsolidate(pailRoot, subDirToProcess, pipelineLabel).run() }
  }
}
