import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window

object dupTrial {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("stagingLoad")
      .setMaster("local[2]")
      .set("spark.executor.memory", "1g")
      .set("spark.debug.maxToStringFields", "10000")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = Seq(
      (1, "james", 23),
      (2, "paul", 30),
      (3, "liz", 28),
      (4, "james", 23),
      (5, "adam", 31),
      (6, "liz", 28),
      (7, "james", 36)
    ).toDF("ID", "last_name", "age")

    df.show()

    //this gives dataframe with duplicate rows
    def findDuplicates(df: DataFrame, col1: String, col2: String): DataFrame = {
    val duplicate_df = df.withColumn("cnt", count("*").over(Window.partitionBy(col1,col2)))
      .where($"cnt">1).drop($"cnt")

      duplicate_df
    }

    findDuplicates(df,"last_name","age").show()

    //this drops the duplicate and orders the row based on ID
    def removeDuplicates(df: DataFrame, col1: String, col2: String, col3: String): DataFrame = {
      val duplicateFree_df =  df.dropDuplicates(col1,col2).orderBy(col3)

      duplicateFree_df
    }

    removeDuplicates(df, "last_name","age","ID").show()


  }


}
