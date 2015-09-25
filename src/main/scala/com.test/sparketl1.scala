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

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.rdd.RDD
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar

case class Person (id: String, depid: String,name: String,sur: String,smtg: String, dept: String,grade: String )


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

    //to do set end date in dw to null //joining on md5 of all cols
    val out = inp_withmd5.leftOuterJoin(snpsht_withmd5).filter(_._2._2 == None).map(_._2._1)
    //making it tuple of no of cols
    val out1 =out.map(x => x.split(",")).map(p => (p(0),p(1),p(2),p(3),p(4),p(5),p(6),"17-Sep-2015","31-Dec-9999"))

    //adding end_dt is null start date as current date


    //val current_day = new SimpleDateFormat()  to add start date based on current date




    //above record  do a inner join with snapshot to identify change recods
    val change_stage1 = out.map(x => (x.split(",")(0), x))
  val tab = sc.textFile(args(2))

    val table = tab.map(x => (x.split(",")(0), x))
//New changed records //not required
   val change = change_stage1.join(table).map(_._2._1)

  //Old changed records  //to do end date in table but first remove existing end_date
    //identify the lates records in table that needs to be endated using max(startdate) = p(7)  Note - Date is kept as numeric like 13072015 is 13 sep 2015
    //chane1 can be cache
  val change1 = change_stage1.join(table).map(_._2._2)

    val change1_latest_rec = change1.map(x => x.split(",")).map(p => (p(0),p(7).toInt)).reduceByKey((x, y) =>  math.max(x, y)).map(p => (p,1)) //setting dummy


    //join with table based on key and end start date

    val final_out_ind = change1.map(x => x.split(",")).map(p => ( (p(0),p(7).toInt),(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),"17-sep-2015")))
    val final_out_changes_existing = final_out_ind.join(change1_latest_rec).map(_._2._1)


    val table_reformat = tab.map(x => x.split(",")).map(p => ( (p(0),p(7).toInt),(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8))))
  // identify un chnged records and old version of hanged records..older thn latest one
     val unchanged = table_reformat.leftOuterJoin(change1_latest_rec).filter(_._2._2 == None).map(_._2._1)

    //But above will also not have the multiple old versions of changed rcords.to add
    //to doo merge final_out1 and final_out_changes_existing using union to create final scd2 type table

    //the final record set is union of 2 changed (new) + new +changed (existing) + not changed (exising)

    val final1 = out1.union(final_out_changes_existing).union(unchanged)


    println(final1.collect().mkString(":::"))
   //final1.saveAsTextFile("table_final")

    //102,456,rajib,,32,mts,A,13082015,31-Dec-2015
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD



    val today = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("mm")
    val currentMinuteAsString = minuteFormat.format(today)
   /* DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();
    val dat1= dateFormat.format(date)*/
    println(today) //2014/08/06 15:59:48

    val test_sql1 = final1.map(p => Person(p._1,p._2,p._3,p._4,p._5,p._6,p._7)).toSchemaRDD

    println(test_sql1.collect().mkString(":::"))
      test_sql1.registerTempTable("test_sql1")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT id FROM test_sql1 WHERE name='rajib'")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    teenagers.map(t => "id: " + t(0)).collect().foreach(println)



  }

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


  def md5Hash(text: String) : String =
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}




 /* def compare(inp: RDD,snpsht:RDD) : RDD = {

    val out = inp.leftOuterJoin
  }

  {} */

}