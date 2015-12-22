package com.test

import org.apache.spark.rdd.RDD

/**
 * Created by abhishek on 2015-12-21.
 */
object test_cdc {

  //import DWops
  def check(rDD: (String,((String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String)])) ) {
    val test = rDD
      //("Sample", ("abhi",None))
    test match {
      case DWops.inserted(test) => print("jiyo")
      case DWops.updated(test) => print("why")


    }

      }
}


trait DWoperations
{ def actual_data : String = "Sample"
  def detect_type(rDD: (String,((String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String)]))) : Boolean
  def unapply (rDD: (String,((String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String)]))) : Option[String] =    //Option [(String,(String,Option[String]))] =
  { if (detect_type(rDD) )
         Some("great")
  else
    None
  }

}

trait Actual_type
{
  def inserted : DWoperations
  def updated : DWoperations

}

class insert extends DWoperations {def detect_type (rDD: (String,((String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String)]))) :Boolean = rDD._2._2 == None }
class update extends DWoperations {def detect_type (rDD: (String,((String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String)]))) :Boolean = 9 > 8 }

object DWops extends Actual_type
{
  //import com.test.insert
  def test_layout ( primary_key : String) : String = "will do meaningful later"


   override val inserted  :  insert  = new insert{ }
   override val updated  : update = new update { }


}
