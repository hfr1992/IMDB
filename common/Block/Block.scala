package common.Block

/**
 * @author Feiran
 * The basic data structure of the system, which stores the data which is to process.
 * Usually we don't directly use the block but use the data structure extends it in the file BlockStream.scala.
 * Attention: Because there is no pointer in Scala, so I use the Byte-array to replace the memory allocation in C++.
 */
class Block (p_BlockSize : Long, p_isReference_ : Boolean, p_memorySpace : Array[Byte]) {
  
  //The size the block
  val BlockSize = p_BlockSize
  //True, if it references to others, which is used in the C++ version code
  val isReference_ = p_isReference_
  //The storage of the block, which is an array of byte
  var memorySpace = p_memorySpace
  
  /**
   * The second constructor of the class, which just need the parameter of the block size.
   */
  def this(blockSize : Long) = {
    this(blockSize, false, new Array[Byte](blockSize.toInt))
  }
  
  /**
   * The third constructor of the class, which copies from a block.
   */
  def this(block : Block) = {
    this(block.BlockSize, block.isReference_, block.memorySpace)
  }
  
  /**
   * Return the storage of the block, which is an array.
   */
  def getBlock() = {
    memorySpace
  }
  
  /**
   * Return the size of the block.
   */
  def getSize() = {
    BlockSize
  }
  
}

//object Block {
//  
//  def main(argv: Array[String]) = {
//    println("Start!");
//    val b1 = new Block(10)
//    val b2 = new Block(20)
//    val b3 = new Block(30)
//    val b4 = new Block(40)
//    println(b1.getBlock().length)
//    println(b2.getBlock().length)
//    println(b3.getBlock().length)
//    println(b4.getBlock().length)
//    println("End!")
//  }
//}