package sqlParser

import scala.util.matching.Regex
import scala.collection.mutable.ArrayBuffer

/**
 * @author Feiran
 * The class is used to parse the SQL query.
 * 
 * Now it supports "select" and "insert" query.
 * Sample1: "select * from groups where id=100"
 * Sample1: "insert into groups(gno,gname,gintro,gmem,cno) values (1,a,b,'c',"d",6), (5,c,d,'g',"55",8),(3,r,y,'5',"3",2)"
 */
class SQLSimpleParser {
  //The type the query, including "select" and "insert"
  var queryType: String = null
  //The name of the table which will be extract from the sentence
  var tableName: String = null
  //The result of the query
  var result = new ArrayBuffer[Array[String]]
  //Whether the parsing is successful
  var parseStatus: Boolean = false
  
  /**
   * Return the name of the table.
   */
  def getTableName() = {
    tableName
  }
  
  /**
   * Return the result of the query.
   */
  def getResult() = {
    result
  }
  
  /**
   * Return the type if the query.
   */
  def getQueryType() = {
    queryType
  }
  
  /**
   * Return the status of the query.
   */
  def getParseStatus() = {
    parseStatus
  }
  
  /**
   * The main parser.
   * str: the SQL sentence. 
   * Using the regular expression to extract the information from the SQL sentence.
   */
  def parser(str: String) = {
    val pattern1 = new Regex("^(S|s)(E|e)(L|l)(E|e)(C|c)(T|t)")
    val pattern2 = new Regex("^(I|i)(N|n)(S|s)(E|e)(R|r)(T|t)")
    val pattern11 = new Regex("=(\\d+)")
    val pattern12 = new Regex("\\d+")
    val pattern13 = new Regex("(F|f)(R|r)(O|o)(M|m)(\\b)+(.)+(\\b)+(W|w)(H|h)(E|e)(R|r)(E|e)")
    val pattern21 = new Regex("(V|v)(A|a)(L|l)(U|u)(E|e)(S|s)(\\b)*(.)*")
    val pattern22 = new Regex("\\([0-9a-zA-Z,'\"]+\\)")
    val pattern23 = new Regex("(I|i)(N|n)(T|t)(O|o)(\\b)+(.)+(V|v)(A|a)(L|l)(U|u)(E|e)(S|s)")
    
    val rs1 = (pattern1 findFirstIn str)
    val rs2 = (pattern2 findFirstIn str)
    
    try {
      if(!rs1.isEmpty){
        //It is a select query.
        queryType = "select"
        val rs11 = (pattern12 findFirstIn ((pattern11 findFirstIn str).get)).get
        val rs12 = (pattern13 findFirstIn str).get
        val rs13 = rs12.substring(4, rs12.length()-5).replace(" ", "")
        
        //The table name: rs13
        tableName = rs13
        //The key number: rs11
        val temp = new Array[String](1)
        temp(0) = rs11
        result.+=(temp)
        parseStatus = true
      }else{
        if(!rs2.isEmpty){
          //It is a insert query.
          queryType = "insert"
          val rs21 = (pattern22 findAllIn ((pattern21 findFirstIn str).get))
          val rs22 = (pattern23 findFirstIn str).get
          var rs23: String = null
          if(rs22.contains("(")){
            rs23 = rs22.substring(4, rs22.indexOf("(")).replace(" ", "")
          }else{
            rs23 = rs22.substring(4, rs22.length()-6).replace(" ", "")
          }
          //The table name: rs23
          tableName = rs23
          //The insert Data
          for(x<-rs21){
            val xx = x.replace("(", "").replace(")", "").replace("'", "").replace("\"", "").split(",")
            result.+=(xx)
          }
          parseStatus = true
        }else{
          println("SQL-parsing error!")
        }
      }
    } catch {
      case ex: Exception => {
        parseStatus = false
        println("SQL-parsing error!")
      } // TODO: handle error
    }
    
  }
}