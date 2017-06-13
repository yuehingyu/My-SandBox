package org.yyinc.sandbox

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import collection.JavaConverters._


// kudu client 
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger
import org.apache.log4j.Level


object Kudu_Test {
  def main(agrs:Array[String]){
    
   
   val client = new KuduClient.KuduClientBuilder("10.0.0.8:7051").build();
   
   client.getTablesList()
    
    
  }
}