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
 * Este programa implementa el conteo de palabras sobre N cantidad de archivos fuente en un directorio. El nombre del
 * directorio donde se encuentran estos archivos es recibido como argumento, y es la carpeta resources que está dentro del
 */
object WordCountInTextFiles {
  /**
   *  Esta función recibe como argumento el nombre de un directorio, itera sobre éste obteniendo los nombres de los archivos
   *  y los devolvuelve en una lista.
   */
  def getListOfFiles(dir: File): List[String] =
    dir.listFiles
      .filter(_.isFile)
      .map(_.getName)
      .toList

  def main(args: Array[String]) {

    // Se inicializa el ambiente de ejecución.
    val env = ExecutionEnvironment.getExecutionEnvironment

    /* Inicializa la variable inputDir, con el valor "resources" que le es pasado como primer argumento. El nombre de este directorio
     * es donde están los archivos fuente
    */
    val inputDir = new File(args(0))

    /* Definimos la salida a archivo para guardar las estadísticas de los archivos que se procesarán.
    */
    val filewriter = new PrintWriter(new File(inputDir+"/out/output.txt"))

    /* Definimos textFiles como una lista de los archivos (fuente). Y también definimos la variable numFiles donde guardaremos
    *  el número de archivos del directorio
    */

    val textFiles = getListOfFiles(inputDir)
    var numFiles = 0

    /* Esta iteración sobre la lista textFiles guardará en el archivo de salida el nombre de los archivos del directorio resources.
    *  la variable numFiles se incrementará en uno para contabilizar el total de archivos.
    */
    for (tf <- textFiles){
      filewriter.write(tf+", ")
      numFiles+=1
    }
    // Terminado de iterar sobre la lista de archivos guardamos el total de archivos en el archivo de salida.
    filewriter.write("#Files("+numFiles+")")

    /* En esta iteración sobre la lista de archivos se obtiene el total de palabras leídas en los archivos,
    *  aplicando la función count sobre la segmentación en palabras realizada por la función flatMap con la expresión regular \\W+.
    *
    *  El conteo de cada archivo se suma al valor de la variable totalWordsRead
    */
    filewriter.write("\n****************\n")
    var totalWordsRead = 0
    for (tf <- textFiles){
      val text = env.readTextFile(inputDir+"/"+tf)
      val counts = text.flatMap{ _.toLowerCase.split("\\W+")}
        .map{(_,1)}
        .count()
      filewriter.write(counts+",")
      totalWordsRead += counts.intValue
    }

    // Terminado esta iteración guardamos el total de palabras de todos los archivos en el archivo de salida.
    filewriter.write("\n****************")
    filewriter.write("\n#TotalWordsRead("+totalWordsRead+")")
    filewriter.write("\n****************")

    /* En esta iteración sobre la lista de archivos, a través de las funciones groupBy (palabra) y sum (para sumarizar por palabra),
    *  obtenemos la recurrencia de palabras de cada archivo y la guardamos en nuestro archivo de salida.
    */
    for (tf <- textFiles){
      filewriter.write("\n"+tf+"\n")
      val text = env.readTextFile(inputDir+"/"+tf)

      val counts = text.flatMap{ _.toLowerCase.split("\\W+")}
        .map { (_, 1) }
        .groupBy(1)
        .sum(1)
      filewriter.write(counts.collect().mkString("\n"))
    }

    // Al término de la ejecución cerramos el archivo de salida
    filewriter.close()
  }
}
