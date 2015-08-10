package common.Block

import scala.collection.mutable.ArrayBuffer

/**
 * @author hfr
 * It need to be synchronized (mutex lock in C++)
 * so I didn't use the mutex lock which is used in the version of C++
 */
class DynamicBlockBuffer() {
  
  var block_list_ = new ArrayBuffer[BlockStreamBase]
  
  def this(r : DynamicBlockBuffer) = {
    this()
    block_list_ = r.block_list_
  }
  
  class Iterator(p_cur_ : Long, p_dbb_ : DynamicBlockBuffer) {
    
    var cur_ : Long = p_cur_
    var dbb_ : DynamicBlockBuffer = p_dbb_
    
    def this() = {
      this(0L, null)
    }
    
    def this(dbb : DynamicBlockBuffer) = {
      this(0L, dbb)
    }
    
    def this(it : Iterator) = {
      this(it.cur_, it.dbb_)
    }
    
    def nextBlock() : BlockStreamBase = {
      var ret: BlockStreamBase = dbb_.getBlock(cur_)
      if(ret!=null){
        cur_ += 1
      }
      ret
    }
    
    /*
     * There used to be a mutex lock
     */
    def atomicNextBlock() : BlockStreamBase = {
      nextBlock()
    }
  }
  
  /* 
   * There is a lock in C++ version in this function
   */
  def appendNewBlock(new_block : BlockStreamBase): Boolean = {
    block_list_.+=(new_block)
    true
  }
  
  def atomicAppendNewBlock(new_block : BlockStreamBase): Boolean = {
    appendNewBlock(new_block)
  }
  
  def getBlock(index : Long) : BlockStreamBase = {
    if(index < block_list_.length){
      block_list_((index.toInt))
    }else{
      null
    }
  }
  
  def createIterator() : Iterator = {
    new Iterator(this)
  }
  
  /*
   * We don't need to free the space because of the JVM garbage collector
   */
//  def destroy() = {
//    
//  }
  
  def getNumberOfBlocks() = {
    block_list_.length
  }
  
  /*
   * Haven't totally confirmed whether the "while" works well
   */
  def getNumberOftuples() = {
    var ret : Long = 0
    var it : Iterator = this.createIterator()
    var block : BlockStreamBase = null
    while( (block = it.nextBlock())!=null ){
      ret+=block.getTuplesInBlock()
    }
    ret
  }
  
}