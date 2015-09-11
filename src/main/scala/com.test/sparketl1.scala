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



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object sparketl1 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[2]"))
    //val threshold = args(1).toInt

    // split each document into words
    val inp = sc.textFile(args(0))


  //TO DO take this value from HBASE
    val seed=200  // lookup file for sequence generator


    val items_and_ids = inp.zipWithIndex()

    // Use a map function to increase the value of the id (the second element in the tuple) by adding the seed to it
    val items_and_ids_mapped = items_and_ids.map(x => (x._2 + seed, x._1))

    //val items_and_ids_mapped = items_and_ids_mapped1.union(seed)

    // Show the output, note that I've move the id to be the first element in the tupl
    println(items_and_ids_mapped.collect().mkString(":::"))

    val out=items_and_ids_mapped.saveAsTextFile(String)

    //System.out.println(charCounts.collect().mkString(", "))
  }
}