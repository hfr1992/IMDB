package common.Block

import common.Schema.Schema
import scala.collection.mutable.ArrayBuffer

/**
 * @author Feiran
 * There are three classes in this file, which are BlockStreamBase, BlockStreamTraverseIterator and BlockStreamVar.
 * The BlockStreamBase is an abstract class which is the subclass of the Block as well as the superclass of the BlockStreamVar, it is constructed as an abstract class because there are two subclass sharing some same methods.
 * The BlockStreamVar is the class which is actually used to store the data.
 * The BlockStreamTraverseIterator is the inner class of the class BlockStreamBase, which is used to traverse the subclass of the BlockStreamBase.
 * 
 * Attention: There is another class named BlockStreamFix in the C++ version of the code, which is used to solve the problem when size of the result exceeds the size of a Block. Now we simplify the system assuming the size of a result is smaller than the size of block.
 */
abstract class BlockStreamBase(block: Block) extends Block(block.BlockSize, block.isReference_, block.memorySpace) {
  
  /**
   * The second constructor providing the size of the block.
   */
  def this(blockSize: Long) {
    this(new Block(blockSize, false, new Array[Byte](blockSize.toInt)))
  }
  
  /**
   * Create the BlockStreamTraverseIterator from the block which needs to be traverse.
   */
  def createIterator() = {
    new BlockStreamTraverseIterator(this)
  }
  
  /**
   * An method needs to be realized in the subclass of the BlockStreamBase.
   */
  protected def getTuple(offset: Long) : Array[Byte]
  
  /**
   * An method needs to be realized in the subclass of the BlockStreamBase.
   */
  def getCurrentPosition() : Long
  
  /**
   * This class is used to traverse the BlockStreamVar.
   * The constructor needs the parameter which block needs traverse.
   */
  class BlockStreamTraverseIterator(p_block_stream_base_ : BlockStreamBase) {
    //The Block which needs traverse.
    private var block_stream_base_ = p_block_stream_base_
    //The current position where it traverse to.
    private var cur = 0L
    //The next record of the result.
    private var next_tuple: Array[Byte] = null
    
    /**
     * To check whether there are more results which haven't been traversed.
     */
    def hasNextTuple() = {
      next_tuple = block_stream_base_.getTuple(cur)
      cur = block_stream_base_.getCurrentPosition()
      next_tuple!=null
    }
    
    /**
     * Return the next tuple after the given index (tuple_off).
     */
    def getTuple(tuple_off : Long) = {
      block_stream_base_.getTuple(tuple_off)
    }
    
    /**
     * Return the next tuple after the index "cur".
     */
    def getNextTuple() = {
      next_tuple
    }
    
    /**
     * Reset the Traverser to the start of the block.
     */
    def reset() = {
      cur = 0L
    }
    
    /**
     * Return the current index where the block is traversed to.
     */
    def get_cur() = {
      cur
    }
    
    /**
     * Set the index where the block is traversed to.
     */
    def set_cur(c : Long) = {
      cur = c
    }
  }
  
}

/** 
 * Some static methods of the class BlockStreamBase.
 */
object BlockStreamBase{
  /**
   * It is used to create the object of the class BlockStreamBase.
   */
  def createBlock(schema : Schema, block : Block) = {
    new BlockStreamVar(block, schema)
  }
}

/**
 * Storage for query result, if each tuple is smaller than the block.
 * If a tuple size is larger than the size of block, we should use the BlockStreamFix to do something.
 * We change it from the origin version of C++ code to simplify it.
 */
class BlockStreamVar(p_block: Block, p_schema: Schema) extends BlockStreamBase(p_block){
  //The schema of the table of DB.
  val schema : Schema = p_schema
  //The current position where the BlockStreamVar is scanned to.
  var cu_pos : Int = 0
  
  /**
   * Return the position where the BlockStreamVar is traversed to.
   */
  def getCurrentPosition() = {
    cu_pos.toLong
  }
  
  /**
   * Get the next tuple from the offset.
   */
  def getTuple(offset: Long): Array[Byte] = {
    var result : ArrayBuffer[Byte] = ArrayBuffer[Byte]()
    cu_pos = offset.toInt
    
    /*
     * The tuple has been totally traversed.
     */
    if(cu_pos>=BlockSize){
      return null
    }
    
    /*
     * If the block hasn't been totaly traversed and it hasn't reach the end of a tuple.
     */
    while((cu_pos<BlockSize)&&(memorySpace(cu_pos).toChar!='~')){
      result.+=(memorySpace(cu_pos))
      cu_pos += 1
    }
    /*
     * If it reach the end of a tuple but not the end of the block.
     */
    if((cu_pos<BlockSize)&&(memorySpace(cu_pos).toChar=='~')){
      cu_pos += 1
    }else{
      return null
    }
    result.toArray
    
  }
}