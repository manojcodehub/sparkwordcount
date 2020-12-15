package Jobs

import org.apache.spark.sql.SparkSession

object sparkOptimisation extends App {

  val spark = SparkSession.builder().master("local[*]").appName("spark_Optimisation")
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "3G")
    .config("spark.driver.memory", "2G")
    .getOrCreate()

  val sparkDF = spark.read.option("header","true").option("inferschema","true")
    .csv("C:\\Hadoop_Learning\\GK Code Labs\\Spark_optimisation").coalesce(4)
  //C:\Hadoop_Learning\GK Code Labs\Spark_optimisation


  val groupdf = sparkDF.groupBy("Issue Date").count()
  sparkDF.show()
  groupdf.show()
  //sparkDF.printSchema()
  System.in.read()
  spark.stop()
}
