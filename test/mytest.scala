package test

import scala.util.Random
import storage.MemoryChunkStore

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
    
    println("**********Automatically generating some random data simulating the Chunk read from HDFS**********\n")
    rePrint(0)
    for( x <- testString ){
      print(x.toChar)
    }
    println("\n")
    
    val memstore: MemoryChunkStore = MemoryChunkStore.getInstance()
    
    memstore.putValue("TestID1", testString)
    
    println("**********Stored the data into Chunk successfully**********\n")
    
    println("**********Getting the Chunk out, start printing......**********\n")
    
    val getChunkOut = memstore.getChunk("TestID1")
    
    for( x <- getChunkOut ){
      print(x.toChar)
    }
    println("\n")
    println("**********Finish getting out**********\n")
    
  }
}