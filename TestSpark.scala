package com.spark

import org.apache.spark.sql.SparkSession


object TestSpark {
  
  
    def main(args: Array[String]): Unit = {
     
      val ss = SparkSession.builder().master("local").getOrCreate();
        import ss.implicits._
        
        val df= ss.read.text("C:/Users/ranaraha/workspace/ScalaProject1/inputdata/input.txt").as[String]
        val words = df.flatMap(value => value.split(",")).groupByKey(_.toString())
        
        
        words.count().show()
        
     

       
       
       
     
      
   }
}