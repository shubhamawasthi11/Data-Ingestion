package processRules

object processFuncs {
  import org.apache.commons.lang.StringUtils
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.functions._
  import sqlContext.implicits._


  //set Invoice Total Cost length
  def checkTotalCost(col1 : String) : Int = {
    if(col1 == None)
      "LF1".toInt   //PRC
    else
      col1.toInt
  }

  //set Invoice Total Price length
  def checkTotalPrice(col1 : String) : Int = {
    if(col1 == None)
      "LF1"   //reject row
    else
      col1.toInt
  }

  //set invoice payment code length
  def checkPaymentCodeLength(col1 : String) : String = {
    if(col1.length() > 256)
      StringUtils.left(col1, 256)
    else
      col1
  }

  //set Invoice Open Date length
  def checkOpenDate(col1 : Any) : Any = {
    if(col1 == None)
      "LF1"   //reject row
    else
      col1
  }

  //set Invoice Close Date length
  def checkCloseDate(col1 : Any) : Any = {
    if(col1 == None)
      "LF1"   //reject row
    else
      col1
  }

  //set Transaction Type length
  def checkTransType(col1 : Any) : Any = {
    if(col1 == None)
      "LF1"   //reject row
    else
      col1
  }

   //set Invoice Open Date length
  def checkOpenDate(col1 : Any) : Any = {
    if(col1 == None)
      "LF1"   //reject row
    else
      col1
  }

  //set wholesale flag
  def setInvRetWholeFlag(col1 : String, col2 : String, col3 : String) : String = {
    if(col1 == "W" && col2 == "0" && col3 != "RepairOrder")
      "W"
    else
      "R"
  }

  //null wholesale flag
  def checkWholeFlagNull(col1 : Any) : Any = {
    if(col1 == None)
      "LF1"   //reject row
    else
      col1
  }

  //set bill number length
  def checkBillNumberLength(col1 : String) : String = {
    if(col1.length() > 32)
      StringUtils.left(col1, 32)
    else
      col1
  }
   //set bill postal code length
  def checkBillPostalCodeLength(col1 : String) : String = {
    if(col1.length() > 60)                    //checks length
      StringUtils.left(col1, 60)        //null-safe substring extractions
    else
      col1
  }

  //set bill number length
  def checkFullNameLength(col1 : String) : String = {
    if(col1.length() > 256)
      StringUtils.left(col1, 256)
    else
      col1
  }

  //set city length
  def checkCityLength(col1 : String) : String = {
    if(col1.length() > 60)
      StringUtils.left(col1, 60)
    else
      col1
  }

  //set ship postal code length
  def checkShipPostalCodeLength(col1 : String) : String = {
    if(col1.length() > 60)                    //checks length
      StringUtils.left(col1, 60)        //null-safe substring extractions
    else
      col1
  }

  //set part number length
  def checkPartNumberLength(col1 : String) : String = {
    if(col1.length() > 40 || col1 != None)
      StringUtils.left(col1, 40)
    else if(col1 == None)
      None
    else
      col1
  }

  //set part number dms length
  def checkPartNumberDMSLength(col1 : String) : String = {
    if(col1.length() > 60)
      StringUtils.left(col1, 60)
    else if(col1 == None)
      "E27"
    else
      col1
  }


  //set Item Qty length
  def checkItemQty(col1 : Any) : Int = {
    if(col1 < -10000 || col1 > 10000 )
      "F15".toInt   //need to reject entire record
    else
      col1
  }

  //set Unit Cost length
  def checkUnitCost(col1 : Any) : String = {
    if(col1 > 20000 )
      0   //need to reject entire record
    else
      col1
  }

  //set Unit Price length
  def checkUnitPrice(col1 : Int) : Int = {
    if(col1 > 20000 )
      StringUtils.left(col1, 60)   //need to reject entire record
    else
      col1
  }

  //set Employee ID length
  def checkEmpIDLength(col1 : String) : Any = {
    if(col1.length() > 30)
      StringUtils.left(col1, 30)
    else
      col1
  }



}