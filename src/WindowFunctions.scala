import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctions {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("Hi").master("local[*]").getOrCreate
    
    // implicits needed to avail toDF functions.
    import spark.implicits._

    // Creating a simple DataFrame for demonstration purposes.
    val customers = spark.sparkContext.parallelize(List(("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).
      toDF("name", "date", "amountSpent")

    // Window Functions

    /* 1. Moving Average.
        rows between -1,1 takes 3 rows at a time and performs their average per partition.
        if first/last row, only 2 values are considered.
    */

    val winSpec = Window.partitionBy("name").orderBy("date").rowsBetween(-1,1)

    customers.withColumn("movingAverages",avg(customers("amountSpent")).over(winSpec)).show()

    /* 2. Cumulative Sum
        rowsBetween(Long.minValue,0) will start processing(in this case, sum) from the minimum value to the
        current row(0).
    */

    val winSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue,0)

    customers.withColumn("cumulativeSum",sum(customers("amountSpent")).over(winSpec2)).show()

    /* 3. Lag = Data from Previous row

    */

    val winSpec3 = Window.partitionBy("name").orderBy("date")

    customers.withColumn("prevAmountSpent",lag(customers("amountSpent"),1).over(winSpec3)).show()

    /* 4. Rank = order of something(in this case, a customers visit)

    */

    customers.withColumn("visitedfor",rank().over(winSpec3)).show()
  }
}
