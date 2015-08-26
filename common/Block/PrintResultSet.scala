package common.Block

/**
 * @author Feiran
 * The class ResultSet is used to store the result set and print it.
 */
class PrintResultSet(r: DynamicBlockBuffer) extends DynamicBlockBuffer(r){
  
  def printRS() = {
    //Get the block list
    for(x<-block_list_){
      val it = x.createIterator()
      var temp : Array[Byte] = null
      //Get each tuple from one block list
      while(it.hasNextTuple()){
        val nextTuple = it.getNextTuple()
        //Get each column from one Tuple
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