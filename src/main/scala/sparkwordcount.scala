
  import org.apache.spark.sql.SparkSession

  object sparkwordcount extends App {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val lines = spark.sparkContext.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))

    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

   // C:\Users\Pavikutty\Downloads
    //val file = spark.sparkContext.textFile("C:\\Hadoop_Learning\\hands on datasets\\Crimes_-_2001_to_present.csv")

    val file = spark.sparkContext.textFile("C:\\Users\\Pavikutty\\Downloads\\Crimes_-_2001_to_present.csv")
    //("C:\\Hadoop_Learning\\hands on datasets\\data-master\\retail_db_json\\orders")
//      C:\Hadoop_Learning\hands on datasets\data-master\retail_db_json\orders
    file.take(5).foreach(println)
    val header = file.first

    val data = file.filter(x => x != header)

     data.take(5).foreach(println)

    val year = data.map(x => x.split(",",-1)(2).split(" ",-1)(0)).distinct.collect.sorted
    year.take(5).foreach(println)
//YYYYMM format, crime count, crime type
    val t = data.map ( rec => {
      val r = rec.split(",")
      val d = r(2).split(",")(0)
      val m = d.split("/")(2) + d.split("/")(0)
      ((m.toInt, r(5)),1)
    }
    )
    //output.take(5).foreach(println)
//t.take(1).foreach(println)

    //header.foreach(println)
    //counts.foreach(println)
    //case 2: top 3 crime types based on number of incidents in RESIDENCE
    val CT = data.map(x=> x.split(","))
    //val crimetype = CT.





  }
