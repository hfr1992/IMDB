package storage

/**
 * @author Suijun_524457
 */

import common._
import catalog._

import scala.collection.mutable.Map

class BlockManager {
  
  var blockInfoPool = Map[String, BlockInfo]()
  var memstore = MemoryChunkStore.getInstance()
  var diskstore = new DiskStore("")
  /*
   * level:
   * 0 - memory
   * 1 - disk
   * 2 - offline
   */
  var storage_Level : Int = -1
  
  class BlockInfo(p_level : Int) {
    var level = p_level
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
//      println("the chunkId is not registered locally, it's on the hdfs!")
//      var ci : ChunkInfo = loadFromHdfs(blockId)
//      put(blockId, 0, ci.hook)
//      ci.hook
    }
    
  }
  
  /* Haven't finished because can't connect to Hdfs
   * 
   */
  def loadFromHdfs(file_name : String) = {
    var pos : Long = file_name.lastIndexOf("$")
    var file_name_former = file_name.substring(0, pos.toInt)
    var file_name_latter = file_name.substring(pos.toInt+1, file_name.length())
    var offset = Integer.parseInt(file_name_latter)
    //Haven't finished
    //new ChunkInfo()
  }
  
  def put(blockId: String, level: Int, value:Array[Byte]) = {
    var bi : BlockInfo = new BlockInfo(level)
    blockInfoPool(blockId) = bi
    
    if(level==0){
      memstore.putValue(blockId, value)
    }
    
    if(level==1){
      diskstore.putValue(blockId, value)
    }
    
    //this function returns true permanently
//    reportBlockStatus(blockId)
    
    true
  }
}

object BlockManager{
  
  var blockmanager : BlockManager = null
  
  def getInstance = {
    if(blockmanager == null){
      blockmanager = new BlockManager()
    }
    blockmanager
  }
}