package storage

import scala.collection.mutable.Map

/**
 * @author hfr
 */
class BlockManager {
  
  var blockInfoPool = Map[String, BlockInfo]()
  var memstore = MemoryChunkStore.getInstance()
  
  /*
   * level:
   * 0 - memory
   * 1 - disk
   * 2 - offline
   */
  class BlockInfo(p_level : Int) {
    var level = p_level
  }
  
  class ChunkInfo() {
    var chunkId : ChunkID
    var hook : Any
  }
  
  def get(blockId : String) = {
    getLocal(blockId)
  }
  
  def getLocal(blockId : String) = {
    var exists = false
    
    if(blockInfoPool.contains(blockId)){
      exists = true
    }
    
    if(exists){
      if(blockInfoPool(blockId).level==0){
        memstore.getChunk(blockId)
      }
//      if(blockInfoPool(blockId).level==1){
//      }
    }else{
      println("the chunkId is not registered locally, it's on the hdfs!")
      ChunkInfo ci = loadFromHdfs(blockId)
      put(blockId, BlockManager.memory, ci.hook)
      ci.hook
    }
    
  }
}

object BlockManager{
  
  def main(argv: Array[String]){
    println("Hello World!")
    
    var temp = new BlockManager()
    println(temp.get("india"))
  }
  
  var blockmanager : BlockManager = null
  
  def getInstance = {
    if(blockmanager == null){
      blockmanager = new BlockManager()
    }
    blockmanager
  }
}