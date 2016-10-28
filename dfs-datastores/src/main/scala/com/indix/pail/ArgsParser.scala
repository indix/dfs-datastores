package com.indix.pail

import org.apache.commons.cli.{CommandLine, Options, PosixParser}


trait ArgsParser {
  val options: Options

  def cmdArgs(name: String)(implicit cli: CommandLine) = {
    if (cli.hasOption(name)) {
      cli.getOptionValue(name)
    } else {
      throw new RuntimeException(s"$name has not been provided")
    }
  }

  def cmdOptionalArgs(name: String)(implicit cli: CommandLine) = {
    if (cli.hasOption(name)) {
      Some(cli.getOptionValue(name))
    } else {
      None
    }
  }

}
