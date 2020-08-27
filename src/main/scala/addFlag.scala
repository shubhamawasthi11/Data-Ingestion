//import libraries
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}


object addFlag {
  def main (args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("stagingLoad")
      .setMaster("local[2]")
      .set("spark.executor.memory", "1g")
      .set("spark.debug.maxToStringFields", "10000")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df_raw = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .load("C:\\Users\\yourfolder\\invoice.txt")


    val df_mapping = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "True")
      .option("inferSchema", "false")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .load("C:\\Users\\yourfolder\\invoiceMapping.csv")


    val colsToRemove = Seq("_c121","_c122","_c123","_c124","_c125","_c126","_c127","_c128","_c129","_c130","_c131",
      "_c132","_c133","_c134","_c135","_c136","_c137","_c138","_c139")

    val df_raw_transformed = df_raw.drop(colsToRemove: _*)


    //extract values of a particular column
    import sqlContext.implicits._

    val columnNames = df_mapping.select("Input").map(r => r(0).asInstanceOf[String]).collect.toList


    //transfer column name to raw data file
    val df_newColumns = df_raw_transformed.toDF(columnNames: _*)


    //get data types from csv file
    val dtypesNames = df_mapping.select("Data Type").map(r => r(0).asInstanceOf[String]).collect.toList



    //mapping column names with new data types
    val type_mapping = columnNames zip dtypesNames


    //mapping new data types to data frame columns
    val newTypes = type_mapping.map(c => df_newColumns.col(c._1).cast(c._2))


    //adding new information to the data frame
    val df_newTypes = df_newColumns.select(newTypes: _*)
    df_newTypes.printSchema()


    val transClass_df = df_newTypes.withColumn("TRANSACTION", when($"REPAIR_ORDER_NUM".isNotNull, "RO")
    .when($"REPAIR_ORDER_NUM".isNull && col("BUSINESS_FLAG") =!= 0, "RC")
    .when($"REPAIR_ORDER_NUM".isNull && col("BUSINESS_FLAG") === 0 && col("WHOLE_FLAG") =!= "W" && ($"TAX".isNotNull || col("TAX") =!= 0), "RC")
    .when($"REPAIR_ORDER_NUM".isNull && col("BUSINESS_FLAG") === 0 && col("WHOLE_FLAG") =!= "W" && ($"TAX".isNull || col("TAX") === 0), "WHL")
    .when($"REPAIR_ORDER_NUM".isNull && col("BUSINESS_FLAG") === 0 && col("WHOLE_FLAG") === "W" && col("TRANSACTION_TYPE") =!= "RepairOrder", "WHL")
    .when($"REPAIR_ORDER_NUM".isNull && col("BUSINESS_FLAG") === 0 && col("WHOLE_FLAG") === "W" && col("TRANSACTION_TYPE") === "RepairOrder", "RC"))


    val final_df = transClass_df.withColumn("WHOLESALE_FLAG", when(col("TRANSACTION") === "RO" || col("TRANSACTION") === "RC", "N")
        .when(col("TRANSACTION") === "WHL", "Y"))


    val coreFlag_df = final_df.withColumn("CORE_FLAG", when(col("PART_NUMBER").substr(10,1) === "U", "Y")
      .when(col("PART_NUMBER").substr(10,1) === "X" && col("QTY") < 0 && col("UNIT_COST") === col("UNIT_PRICE") && col("UNIT_COST") =!= col("NET_COST"), "Y")
      .when(($"PART_DESC".like("%BATTERY%") || $"PART_DESC".like("%CORE RETURN%")) && col("QTY") > 0 && col("CORE_QTY") < 0 && col("UNIT_CORE_COST") =!= 0 && col("UNIT_CORE_PRICE") =!= 0, "Y")
      .otherwise("N"))

    val y = Seq("PART_NUMBER", "PART_DESC", "QTY", "CORE_QTY", "UNIT_COST", "NET_COST", "UNIT_PRICE", "CORE_FLAG")
    coreFlag_df.select(y.head, y.tail: _*).show(100, false)

    coreFlag_df.filter($"CORE_FLAG" === "Y").show()
  }

}
