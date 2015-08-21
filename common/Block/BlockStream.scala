package common.Block

import common.Schema.Schema
import scala.collection.mutable.ArrayBuffer

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
      cur = block_stream_base_.getCurrentPosition()
      result
    }
    
    def currentTuple() = {
      block_stream_base_.getTuple(cur)
    }
    
    def getTuple(tuple_off : Long) = {
      block_stream_base_.getTuple(tuple_off)
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
  
  protected def getTuple(offset: Long) : Array[Byte]
  
  def getCurrentPosition() : Long
  
}

/* Some static methods.
 * Very important.
 */
object BlockStreamBase{
  /* Haven't been realized.
   * Extract data from Schema to Block, which is a key step.
   */
  def createBlock(schema : Schema, block : Block) = {
    new BlockStreamVar(block, schema)
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
class BlockStreamVar(p_block: Block, p_schema: Schema) extends BlockStreamBase(p_block){
  val schema : Schema = p_schema
  var cu_pos : Int = 0
//  var cur_tuple_size_ : Long = 0L
//  var var_attributes_ : Long = 0L
  
  def getCurrentPosition() = {
    cu_pos.toLong
  }
  
  def getTuple(offset: Long): Array[Byte] = {
    var result : ArrayBuffer[Byte] = null
    cu_pos = offset.toInt
    
    if(cu_pos>=BlockSize){
      return null
    }
    
    while((memorySpace(cu_pos).toChar!='~')&&(cu_pos<BlockSize)){
      result.+=(memorySpace(cu_pos))
      cu_pos += 1
    }
    if(memorySpace(cu_pos).toChar!='~'){
      result = null
    }
    result.toArray
    
  }
  
//  def constructFromBlock(block : Block, p_actual_size: Long, p_tuple_size: Long) = {
//    this.memorySpace = block.memorySpace
//    actual_size = p_actual_size
//    tuple_size = p_tuple_size
//  }
}