package com.test

import org.apache.spark.rdd.RDD

/**
 * Created by abhishek on 2015-12-21.
 */
object test_cdc {

  //import DWops
  def main(args: Array[String]) {
    val test = "Sample"
    test match {
      case DWops.inserted => print("jiyo")
      case DWops.updated => print("why")


    }

  }
}


trait DWoperations
{ def actual_data : String = "Sample"
  def detect_type : Boolean
  def unapply (rDD: String)
  { if (detect_type )
         actual_data
  else
    None
  }

}

trait Actual_type
{
  def inserted : DWoperations
  def updated : DWoperations

}

class insert extends DWoperations {def detect_type() :Boolean = 4 > 2 }
class update extends DWoperations {def detect_type() :Boolean = 5 > 8 }

object DWops extends Actual_type
{
  //import com.test.insert
  def test_layout ( primary_key : String) : String = "will do meaningful later"


   override val inserted  :  insert  = new insert{ }
   override val updated  : update = new update { }


}
