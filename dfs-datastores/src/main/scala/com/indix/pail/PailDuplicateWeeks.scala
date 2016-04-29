package com.indix.pail

import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object PailDuplicateWeeks extends App {
  val cmdArgs = Args(args)

  val pricesBase = cmdArgs("input")

  val path = new Path(pricesBase)
  val fs = path.getFileSystem(new Configuration())
  val weeks = fs.listStatus(path)

  weeks.foreach(week => {
    val name = week.getPath.getName

    val copiedPath = new Path(week.getPath.toString, name)
    val copiedWeeks = fs.exists(copiedPath)
    if (copiedWeeks) {
      println(s"${week.getPath} is copied over recursively as ${copiedPath}")
    }
  })
}
