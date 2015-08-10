package common.Block

import common.Schema.Schema

/**
 * @author hfr
 */
abstract class BlockStreamBase(block: Block) extends Block(block.BlockSize, block.isReference_, block.memorySpace) {
  
  def this(blockSize: Long) {
    this(new Block(blockSize, false, new Array[Byte](blockSize.toInt)))
  }
  
  class BlockStreamTraverseIterator(p_block_stream_base_ : BlockStreamBase) {
    private var block_stream_base_ = p_block_stream_base_
    private var cur = 0L
    
    def nextTuple() = {
      val result = block_stream_base_.getTuple(cur)
      cur+=1
      result
    }
    
    def currentTuple() = {
      block_stream_base_.getTuple(cur)
    }
    
    def getTuple(tuple_off : Long) = {
      block_stream_base_.getTuple(tuple_off)
    }
    
    def increase_cur_() = {
      cur+=1
    }
    
    def reset() = {
      cur = 0L
    }
    
    def get_cur() = {
      cur
    }
    
    def set_cur(c : Long) = {
      cur = c
    }
  }
  
  def createBlockAndDeepCopy() : BlockStreamBase
  
  def allocateTuple(bytes : Long)
  
  def setEmpty()
  
  def Empty(): Boolean
  
  def Full(): Boolean
  
  //Useless because there is no pointer in Scala
  //def getBlockDataAddress()
  
  def getTuplesInBlock(): Long
  
  def getBlockCapacityInTuples(): Long
  
  //Changed
  def copyBlock(block : Block)
  
  //Useless
  //def insert()
  
  //Useless
  def deepCopy(block : Block)
  
  def constructFromBlock(block : Block)
  
  def switchBlock(block : BlockStreamBase)
  
  def serialize(block : Block)
  
  def deserialize(block : Block)
  
  def getSerialiedBlockSize() : Long
  
  def createIterator() = {
    new BlockStreamTraverseIterator(this)
  }
  
  protected def getTuple(offset : Long)
  
}

/* Some static methods.
 * Very important.
 */
object BlockStreamBase{
  /* Haven't been realized.
   * Extract data from Schema to Block.
   * It's a key step.
   */
  def createBlock(schema : Schema, block_size : Long) = {
    
  }
  
  //Haven't been realized.
  def createBlockWithDesirableSerilaizedSize(schema : Schema, block_size : Long) = {
    
  }
}