package org.yyinc.sandbox


import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.log4j.Logger
import org.apache.log4j.Level



object CRH_Kudu {
  def main(args: Array[String]) {
   
   val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
   .builder()
   .appName("CRH Examples")
   .config("spark.sql.warehouse.dir", warehouseLocation)
   .master("local[*]") 
   .enableHiveSupport()
   .getOrCreate()
   
   
   spark.sparkContext.getConf.getAll.foreach(println)
   
   spark.sparkContext.setLogLevel("ERROR")
   
   
   spark.sql("show databases").show()
   
   // use retail database
   
   spark.sql("use retail")
   
    
   // example to create, load to a table  and summarize desired report
   
    val complaintDF = spark.read.json("/tmp/customer_complaints.json")
    
    complaintDF.show()
    
    
    // this column does not shown when using spark-shell complaintDF.drop("_corrupt_record")
    
    
    complaintDF.drop("_corrupt_record")
    
    
    complaintDF.printSchema()

    complaintDF.createOrReplaceTempView("my_temp_table_view")

    
    
    spark.sql("DROP TABLE IF EXISTS customer_complaint") 
    
    
    // create table and insert at the same time.. lazy way
    
    spark.sql("CREATE TABLE customer_complaint STORED AS ORC  AS SELECT * from my_temp_table_view")
    
    
    
    spark.sql("INSERT INTO customer_complaint_v2 SELECT company,complaint_id,timely_response from my_temp_table_view")
    
    
    spark.sql("select * from customer_complaint").show()
    
    
    // show response time for each company, order by # of complaints and its timely and non timely response
    
    
    val summarySQL="select a.company as company, a.case as case, b.case as timely_response, c.case as non_timely_response from (select company,count(*) as case from customer_complaint group by company) as a inner join (select company,count(*) as case from customer_complaint where timely_response='Yes' group by company) as b inner join (select company,count(*) as case from customer_complaint where timely_response='No' group by company) as c where a.company=b.company and a.company==c.company order by case desc"
    
    val summaryResult=spark.sql(summarySQL)
    
    summaryResult.show()
    
    printf("Total row : "+summaryResult.count())
    
    
    // create table first and insert data from the temp view table.. spark sql does not support CLUSTERED by ..
    
    
    //val sqlSTMT="CREATE TABLE customer_complaint_v2 (company string, complaint_id string,timely_response string) stored as ORC"
    
    
    //spark.sql("DROP TABLE IF EXISTS customer_complaint_v2")
    
    //spark.sql(sqlSTMT)
    
/*
 * 
 * CREATE TABLE customer_complaint_v2 (
company                        string,
complaint_id                   string,
timely_response                string
)
CLUSTERED BY (complaint_id) INTO 2 BUCKETS STORED AS ORC
TBLPROPERTIES ("transactional"="true",
  "compactor.mapreduce.map.memory.mb"="2048",
  "compactorthreshold.hive.compactor.delta.num.threshold"="4",
  "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5"
)

* */
    
    
    spark.sql("INSERT INTO customer_complaint_v2 SELECT company,complaint_id,timely_response from my_temp_table_view")
    
    val summarySQL2="select a.company as company, a.case as case, b.case as timely_response, c.case as non_timely_response from (select company,count(*) as case from customer_complaint_v2 group by company) as a inner join (select company,count(*) as case from customer_complaint_v2 where timely_response='Yes' group by company) as b inner join (select company,count(*) as case from customer_complaint_v2 where timely_response='No' group by company) as c where a.company=b.company and a.company==c.company order by case desc"
    
    val summaryResult2=spark.sql(summarySQL2)
    
    summaryResult2.show()

    printf("Total row : "+summaryResult2.count())

    val count=spark.sql("select count(*) from customer_complaint_v2").head().getLong(0)
    
    printf("Total rows : "+count)
   
    
    
      
  }
}