package common.Block

/**
 * @author hfr
 * Because of the different ways of memory management, there are some tiny differences in this class.
 */
class BlockContainer(size : Long) extends Block(size){
  
  var actual_size = 0L
  
  def GetMaxSize() = {
    getSize()
  }
  
  def GetCurSize() = {
    actual_size
  }
  
  def GetRestSize() = {
    getSize() - actual_size
  }
  
  def reset() = {
    actual_size = 0L
  }
  
  def IncreseActualSize(size : Long) = {
    if(size+GetCurSize()>getSize()){
      throw new IllegalArgumentException
    }
    actual_size += size
  }
  
  def copy(block : BlockContainer) = {
    if(getSize()!=block.getSize()){
      throw new IllegalArgumentException
    }
    this.memorySpace = block.getBlock().clone()
    actual_size = block.GetCurSize()
  }
}