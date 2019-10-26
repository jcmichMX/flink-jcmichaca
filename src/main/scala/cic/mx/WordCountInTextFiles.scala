package cic.mx

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import java.io._


/**
 *
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCountInTextFiles {

  def getListOfFiles(dir: File): List[String] =
    dir.listFiles
      .filter(_.isFile)
      .map(_.getName)
      .toList

  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filewriter = new PrintWriter(new File("resources/out/output.txt"))

    val inputDir = new File(args(0))

    val textFiles = getListOfFiles(inputDir)
    var numFiles = 0
    for (tf <- textFiles){
      filewriter.write(tf+", ")
      numFiles+=1
    }
    filewriter.write("#Files("+numFiles+")")

    filewriter.write("\n****************\n")
    var totalWordsRead = 0
    for (tf <- textFiles){
      val text = env.readTextFile(args(0)+"/"+tf)
      val counts = text.flatMap{ _.toLowerCase.split("\\W+")}
        .map{(_,1)}
        .count()
      filewriter.write(counts+",")
      totalWordsRead += counts.intValue
    }

    filewriter.write("\n****************")
    filewriter.write("\n#TotalWordsRead("+totalWordsRead+")")
    filewriter.write("\n****************")


    for (tf <- textFiles){
      filewriter.write("\n"+tf+"\n")
      val text = env.readTextFile(args(0)+"/"+tf)

      val counts = text.flatMap{ _.toLowerCase.split("\\W+")}
        .map { (_, 1) }
        .groupBy(1)
        .sum(1)
      filewriter.write(counts.collect().mkString("\n"))
    }
    filewriter.close()
  }
}
