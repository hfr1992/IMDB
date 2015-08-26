package common.Block

import scala.collection.mutable.ArrayBuffer

/**
 * @author Feiran
 * This class is used to store a set of the results with the ArrayBuffer[BlockStreamBase]
 * It need to be synchronized (it is mutex lock realized in C++)
 */
class DynamicBlockBuffer() {
  
  //The data structure is used to store the set of the result.
  var block_list_ = new ArrayBuffer[BlockStreamBase]
  
  /**
   * The second constructor constructing the class using an existing class.
   */
  def this(r : DynamicBlockBuffer) = {
    this()
    block_list_ = r.block_list_
  }
  
  /**
   * The class is used to traverse the DynamicBlockBuffer.
   */
  class Iterator(p_cur_ : Long, p_dbb_ : DynamicBlockBuffer) {
    //Current position where it traverses to.
    var cur_ : Long = p_cur_
    //The DynamicBlockBuffer which iterator built on.
    var dbb_ : DynamicBlockBuffer = p_dbb_
    
    /**
     * The constructor without any parameter.
     */
    def this() = {
      this(0L, null)
    }
    
    /**
     * The constructor with the DynamicBlockBuffer.
     */
    def this(dbb : DynamicBlockBuffer) = {
      this(0L, dbb)
    }
    
    /**
     * The constructor with the existing iterator.
     */
    def this(it : Iterator) = {
      this(it.cur_, it.dbb_)
    }
    
    /**
     * Return the next Block in the BlockList.
     */
    def nextBlock() : BlockStreamBase = {
      var ret: BlockStreamBase = dbb_.getBlock(cur_)
      if(ret!=null){
        cur_ += 1
      }
      ret
    }
    
    /**
     * Return the next block.
     * There used to be a mutex lock
     */
    def atomicNextBlock() : BlockStreamBase = {
      nextBlock()
    }
  }
  
  /**
   * Add a new block to the block list.
   */
  def appendNewBlock(new_block : BlockStreamBase): Boolean = {
    block_list_.+=(new_block)
    true
  }
  
  /**
   * Add a new block to the block list.
   */
  def atomicAppendNewBlock(new_block : BlockStreamBase): Boolean = {
    appendNewBlock(new_block)
  }
  
  /**
   * Return the Block at a certain position in the block list.
   */
  def getBlock(index : Long) : BlockStreamBase = {
    if(index < block_list_.length){
      block_list_((index.toInt))
    }else{
      null
    }
  }
  
  /**
   * Create a new iterator from the DynamicBlockBuffer.
   */
  def createIterator() : Iterator = {
    new Iterator(this)
  }
  
  /**
   * Return the size of the BlockList.
   */
  def getNumberOfBlocks() = {
    block_list_.length
  }
  
}