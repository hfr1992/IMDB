package sqlParser

import scala.util.matching.Regex
import scala.collection.mutable.ArrayBuffer

/**
 * @author hfr
 */
class SQLSimpleParser {
  var queryType: String = null
  var tableName: String = null
  var result = new ArrayBuffer[Array[String]]
  var parseStatus: Boolean = false
  
  def getTableName() = {
    tableName
  }
  
  def getResult() = {
    result
  }
  def getQueryType() = {
    queryType
  }
  def getParseStatus() = {
    parseStatus
  }
  
  def parser(str: String) = {
    val pattern1 = new Regex("^(S|s)(E|e)(L|l)(E|e)(C|c)(T|t)")
    val pattern2 = new Regex("^(I|i)(N|n)(S|s)(E|e)(R|r)(T|t)")
    val pattern11 = new Regex("=(\\d+)")
    val pattern12 = new Regex("\\d+")
    val pattern13 = new Regex("(F|f)(R|r)(O|o)(M|m)(\\b)+(.)+(\\b)+(W|w)(H|h)(E|e)(R|r)(E|e)")
    val pattern21 = new Regex("(V|v)(A|a)(L|l)(U|u)(E|e)(S|s)(\\b)*(.)*")
    val pattern22 = new Regex("\\([0-9a-zA-Z,'\"]+\\)")
    val pattern23 = new Regex("(I|i)(N|n)(T|t)(O|o)(\\b)+(.)+(V|v)(A|a)(L|l)(U|u)(E|e)(S|s)")
    //SQL String for test
//    val str = "select * from groups where id=100"
//    val str = "insert into groups(gno,gname,gintro,gmem,cno) values (1,a,b,'c',\"d\",6), (5,c,d,'g',\"55\",8),(3,r,y,'5',\"3\",2)"
    
    val rs1 = (pattern1 findFirstIn str)
    val rs2 = (pattern2 findFirstIn str)
    
    try {
      if(!rs1.isEmpty){
  //      println("SQL type: 'select'.")
        queryType = "select"
        val rs11 = (pattern12 findFirstIn ((pattern11 findFirstIn str).get)).get
        val rs12 = (pattern13 findFirstIn str).get
        val rs13 = rs12.substring(4, rs12.length()-5).replace(" ", "")
        
  //      println("The table name: \n"+rs13)
        tableName = rs13
  //      println("The key number: \n"+rs11)
        val temp = new Array[String](1)
        temp(0) = rs11
        result.+=(temp)
        parseStatus = true
      }else{
        if(!rs2.isEmpty){
  //        println("SQL type: 'insert'.")
          queryType = "insert"
          val rs21 = (pattern22 findAllIn ((pattern21 findFirstIn str).get))
          val rs22 = (pattern23 findFirstIn str).get
          var rs23: String = null
          if(rs22.contains("(")){
            rs23 = rs22.substring(4, rs22.indexOf("(")).replace(" ", "")
          }else{
            rs23 = rs22.substring(4, rs22.length()-6).replace(" ", "")
          }
  //        println("The table name: \n"+rs23)
          tableName = rs23
  //        println("Insert Data: ")
          for(x<-rs21){
            val xx = x.replace("(", "").replace(")", "").replace("'", "").replace("\"", "").split(",")
  //          for(xxx<-xx){
  //            print(xxx+" ")
  //          }
  //          println("")
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