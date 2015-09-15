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


//import com.test.CDC_check
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object sparketl1 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[2]"))
    //val threshold = args(1).toInt

    // split each document into words
    val inp = sc.textFile(args(0))

    val snpsht = sc.textFile(args(1))

    val snpsht1 = snpsht.map(x => (x.split(",")(0), x))

    val snpsht_withmd5 = snpsht1.mapValues(x =>(x, md5Hash( x))).map(_._2).map{ case (x,y )  => (y,x)}


    val inp1 = inp.map(x => (x.split(",")(0), x))

    val inp_withmd5 = inp1.mapValues(x =>(x, md5Hash( x))).map(_._2).map{ case (x,y )  => (y,x)}


    //identify new or changed records

    //to do set end date in dw to null
    val out = inp_withmd5.leftOuterJoin(snpsht_withmd5).filter(_._2._2 == None).map(_._2._1)


    //above record  do a inner join with snapshot to identify change recods
    val change_stage1 = out.map(x => (x.split(",")(0), x))

//New changed records
   val change = change_stage1.join(snpsht1).map(_._2._1)

  //Old changed records sanpshot //to do end date
  val change1 = change_stage1.join(snpsht1).map(_._2._2)








   // val snpsht1 = snpsht.map(x => (x.split(",")(0), x))

     // inp.map(line => line.split(",")).map({line => (line(0),line))

   // println(snpsht_withmd5.collect().mkString(":::")()

    println(change.collect().mkString(":::"))

   /*def test_change(inp: RDD) : Boolean =
    { val test = inp.map{case (key, value) => value }.map {case (key, value) => value}
      val check= test:RDD[Any].filter

    } */

      //TO DO take this value from HBASE
   // val seed=200  // lookup file for sequence generator

/*  to implement suurogate key
    val items_and_ids = inp.zipWithIndex()

    // Use a map function to increase the value of the id (the second element in the tuple) by adding the seed to it
    val items_and_ids_mapped = items_and_ids.map(x => (x._2 + seed, x._1))  */

    //val items_and_ids_mapped = items_and_ids_mapped1.union(seed)

    // Show the output, note that I've move the id to be the first element in the tupl
   // println(items_and_ids_mapped.collect().mkString(":::"))
    //val cdc1 = new CDC_check

    //val oup = ()
    //val out=items_and_ids_mapped.saveAsTextFile(String)

    //System.out.println(charCounts.collect().mkString(", "))
  }

  def md5Hash(text: String) : String =
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}




 /* def compare(inp: RDD,snpsht:RDD) : RDD = {

    val out = inp.leftOuterJoin
  }

  {} */

}