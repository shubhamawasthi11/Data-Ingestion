import org.apache.spark.sql.DataFrame

class replaceColumn {

  def replaceColumn(df_mapping: DataFrame, df_raw: DataFrame): DataFrame = {
    import sqlContext.implicits._
    //first index first column to extract column names
    //second is to treat that column names in a list as individual arguments
    //third is to push those column names to raw dataframe
    var df =  df_raw.toDF(df_mapping.select(df_mapping.columns(0)).map(r => r(0).asInstanceOf[String]).collect.toList: _*)

    //used var as this dataframe variable will be modified multiple times
    //return final dataframe
    df
  }

}

