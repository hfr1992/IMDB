package common.Block

/**
 * @author hfr
 */
class ResultSet(r: DynamicBlockBuffer) extends DynamicBlockBuffer(r){
  
  def printRS() = {
    for(x<-block_list_){
      val it = x.createIterator()
      var temp : Array[Byte] = null
      while(it.hasNextTuple()){
        val nextTuple = it.getNextTuple()
        for( x<-nextTuple ){
          if(x.toChar=='^'){
            print("\t")
          }else{
            print(x.toChar.toString())
          }
        }
        println()
      }
    }
  }
}