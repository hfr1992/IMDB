package common.Block

/**
 * @author hfr
 */
class ResultSet(r: ResultSet) extends DynamicBlockBuffer(r){
  def print() = {
    for(x <- block_list_){
      val it = x.createIterator()
      var temp : Array[Byte] = null
      while( (temp = it.nextTuple())!=null ){
        println(temp)
      }
    }
  }
}