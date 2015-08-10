package test

import scala.collection.mutable.ArrayBuffer

/**
 * @author hfr
 */
class mytest {
  
}

object mytest {
  
  def testFunction(test4 : Boolean) : ArrayBuffer[Int] = {
    if(test4){
      null
    }else{
      new ArrayBuffer[Int]
    }
  }
  
  def main(args: Array[String]){
    println("Hello world!")
    
    var test3 = false
    var test2 : ArrayBuffer[Int] = null
    println( test2 = (new ArrayBuffer[Int]+=1) )
  }
}