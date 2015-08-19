package test

import scala.util.Random
import storage.MemoryChunkStore
import dbConnection.DBConnection

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
    
    val DBCon = new DBConnection()
    
    val rs = DBCon.query("select * from groups;")
    while(rs.next()){
      println(rs.getString("gno")+" "+rs.getString("gname")+" "+rs.getString("gnum")+" "+rs.getString("gintro")+" "+rs.getString("cno"))
    }
    
    DBCon.insert("insert into groups values (100, 'hello_infosys', '1', 'Is this a group?', '11111111');")
    
    val rs2 = DBCon.query("select * from groups;")
    while(rs2.next()){
      println(rs2.getString("gno")+" "+rs2.getString("gname")+" "+rs2.getString("gnum")+" "+rs2.getString("gintro")+" "+rs2.getString("cno"))
    }
    
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