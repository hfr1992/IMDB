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
  
//  def createBlockAndDeepCopy() : BlockStreamBase
//  
//  def allocateTuple(bytes : Long)
//  
//  def setEmpty()
//  
//  def Empty(): Boolean
//  
//  def Full(): Boolean
//  
//  //Useless because there is no pointer in Scala
//  //def getBlockDataAddress()
//  
//  def getTuplesInBlock(): Long
//  
//  def getBlockCapacityInTuples(): Long
//  
//  //Changed
//  def copyBlock(block : Block)
//  
//  //Useless
//  //def insert()
//  
//  //Useless
//  def deepCopy(block : Block)
//  
//  def constructFromBlock(block : Block, a_size: Long)
//  
//  def switchBlock(block : BlockStreamBase)
//  
//  def serialize(block : Block)
//  
//  def deserialize(block : Block)
//  
//  def getSerialiedBlockSize() : Long
  
  def createIterator() = {
    new BlockStreamTraverseIterator(this)
  }
  
  protected def getTuple(offset : Long) : Array[Byte]
  
}

/* Some static methods.
 * Very important.
 */
object BlockStreamBase{
  /* Haven't been realized.
   * Extract data from Schema to Block, which is a key step.
   */
  def createBlock(schema : Schema, block : Block, p_actual_size: Long, p_tuple_size: Long) = {
    new BlockStreamVar(block, schema, p_actual_size, p_tuple_size)
  }
  
  //Haven't been realized.
//  def createBlockWithDesirableSerilaizedSize(schema : Schema, block_size : Long) = {
//    
//  }
}

/**
 * Storage for query result, if each tuple is smaller than the block.
 * If a tuple size is larger than the size of block, we should use the BlockStreamFix to do something.
 * We change it from the origin version of CLAIMS to simplify it.
 */
class BlockStreamVar(p_block: Block, p_schema: Schema, p_actual_size: Long, p_tuple_size: Long) extends BlockStreamBase(p_block){
  val schema : Schema = p_schema
  //The length of the data stored in the Block. It needs to be initialized!!!!!!!!!
  var actual_size : Long = p_actual_size
  //The size of the tuple stored in the Block. It needs to be initialized!!!!!!!!!
  var tuple_size : Long = p_tuple_size
//  var cur_tuple_size_ : Long = 0L
//  var var_attributes_ : Long = 0L
  
  def getTuple(offset: Long) = {
    if( offset < actual_size/tuple_size ){
      var temp = new Array[Byte](tuple_size.toInt)
      var counter = offset*tuple_size
      while(counter<(offset+1)*tuple_size){
        temp((counter-offset*tuple_size).toInt) = memorySpace(counter.toInt)
      }
      temp
    }else{
      null
    }
  }
  
//  def constructFromBlock(block : Block, p_actual_size: Long, p_tuple_size: Long) = {
//    this.memorySpace = block.memorySpace
//    actual_size = p_actual_size
//    tuple_size = p_tuple_size
//  }
}