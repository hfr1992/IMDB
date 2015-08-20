package test

import scala.util.Random
import storage.MemoryChunkStore
import dbConnection.DBConnection
import scala.util.matching.Regex

/**
 * @author hfr
 */
class mytest {
  
}

object mytest {
  val testStringLength = 10000
  val ran = new Random()
  val testString = new Array[Byte](testStringLength)
  
  def rePrint(time: Int) {
    val time2 = time + 1
    if(time < testStringLength){
      testString(time) = ran.nextPrintableChar().toByte
      rePrint(time2)
    }
  }
  
  def main(args :Array[String]){
    
    val pattern1 = new Regex("^(S|s)(E|e)(L|l)(E|e)(C|c)(T|t)")
    val pattern2 = new Regex("^(I|i)(N|n)(S|s)(E|e)(R|r)(T|t)")
    val pattern11 = new Regex("(I|i)(D|d)=(\\d+)")
    val pattern12 = new Regex("\\d+")
    val pattern13 = new Regex("(F|f)(R|r)(O|o)(M|m)(\\b)+(.)+(\\b)+(W|w)(H|h)(E|e)(R|r)(E|e)")
    val pattern21 = new Regex("(V|v)(A|a)(L|l)(U|u)(E|e)(S|s)(\\b)*(.)*")
    val pattern22 = new Regex("\\([0-9a-zA-Z,'\"]+\\)")
    val pattern23 = new Regex("(I|i)(N|n)(T|t)(O|o)(\\b)+(.)+(V|v)(A|a)(L|l)(U|u)(E|e)(S|s)")
//    val str = "select * from groups where id=100"
    val str = "insert into groups(gno,gname,gintro,gmem,cno) values (1,a,b,'c',\"d\",6), (5,c,d,'g',\"55\",8),(3,r,y,'5',\"3\",2)"
    
    val rs1 = (pattern1 findFirstIn str)
    val rs2 = (pattern2 findFirstIn str)
    
    if(!rs1.isEmpty){
      println("Select Query get!\n")
      val rs11 = (pattern12 findFirstIn ((pattern11 findFirstIn str).get)).get
      val rs12 = (pattern13 findFirstIn str).get
      val rs13 = rs12.substring(4, rs12.length()-5).replace(" ", "")
      println("The table name: \n"+rs13)
      println("The ID number: \n"+rs11)
    }else{
      if(!rs2.isEmpty){
        println("Insert Query get!\n")
        val rs21 = (pattern22 findAllIn ((pattern21 findFirstIn str).get))
        val rs22 = (pattern23 findFirstIn str).get
        var rs23: String = null
        if(rs22.contains("(")){
          rs23 = rs22.substring(4, rs22.indexOf("(")).replace(" ", "")
        }else{
          rs23 = rs22.substring(4, rs22.length()-6).replace(" ", "")
        }
          println("The table name: \n"+rs23)
        println("Insert Data: ")
        for(x<-rs21){
          val xx = x.replace("(", "").replace(")", "").replace("'", "").replace("\"", "").split(",")
          for(xxx<-xx){
            print(xxx+" ")
          }
          println("")
        }
      }else{
        println("SQL error!")
      }
    }
    
//    val rs1 = (pattern1 findFirstIn str)
//    println(rs1.isEmpty)
//    println(rs1.get)
//    val rs2 = (pattern3 findFirstIn ((pattern2 findFirstIn str).get))
//    println(rs2.isEmpty)
//    println(rs2.get)
    
//    val DBCon = new DBConnection()
//    
//    val rs = DBCon.query("select * from groups;")
//    while(rs.next()){
//      println(rs.getString("gno")+" "+rs.getString("gname")+" "+rs.getString("gnum")+" "+rs.getString("gintro")+" "+rs.getString("cno"))
//    }
//    
//    DBCon.insert("insert into groups values (100, 'hello_infosys', '1', 'Is this a group?', '11111111');")
//    
//    val rs2 = DBCon.query("select * from groups;")
//    while(rs2.next()){
//      println(rs2.getString("gno")+" "+rs2.getString("gname")+" "+rs2.getString("gnum")+" "+rs2.getString("gintro")+" "+rs2.getString("cno"))
//    }
    
//    println("**********Automatically generating some random data simulating the Chunk read from HDFS**********\n")
//    rePrint(0)
//    for( x <- testString ){
//      print(x.toChar)
//    }
//    println("\n")
//    
//    val memstore: MemoryChunkStore = MemoryChunkStore.getInstance()
//    
//    memstore.putValue("TestID1", testString)
//    
//    println("**********Stored the data into Chunk successfully**********\n")
//    
//    println("**********Getting the Chunk out, start printing......**********\n")
//    
//    val getChunkOut = memstore.getChunk("TestID1")
//    
//    for( x <- getChunkOut ){
//      print(x.toChar)
//    }
//    println("\n")
//    println("**********Finish getting out**********\n")
    
  }
}