package common.Block

/**
 * @author hfr
 */
class Block (p_BlockSize: Long, p_isReference: Boolean, p_memorySpace: Array[Byte]) {
  val BlockSize = p_BlockSize
  val isReference = p_isReference
  var memorySpace = p_memorySpace
  
  def this(blockSize: Long) = {
    this(blockSize, false, new Array[Byte](blockSize.toInt))
  }
  
  def this(block: Block) = {
    this(block.BlockSize, block.isReference, block.memorySpace)
  }
  
  def getBlock() = {
    memorySpace
  }
  
  def getSize() = {
    BlockSize
  }
  
}

object Block {
  
  def main(argv: Array[String]) = {
    println("Start!");
    val b1 = new Block(10)
    val b2 = new Block(20)
    val b3 = new Block(30)
    val b4 = new Block(40)
    println(b1.getBlock().length)
    println(b2.getBlock().length)
    println(b3.getBlock().length)
    println(b4.getBlock().length)
    println("End!")
  }
}