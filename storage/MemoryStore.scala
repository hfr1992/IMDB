package storage

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

/**
 * @author hfr
 */
class MemoryChunkStore {
  
  var bufferpool = Map[String, HdfsBlock]()
//  var chunk_pool = new ArrayBuffer
//  var block_pool = new ArrayBuffer
  
  class HdfsBlock(p_add: Any, p_length: Int){
    var hook: Any = p_add
    var length = p_length
    
    def this(){
      this(null, 0)
    }
  }
  
  def getChunk(blockId: String) = {
    if(bufferpool.contains(blockId)){
      bufferpool(blockId).hook
    }else{
      null
    }
  }
  
  def putValue(chunkId: String, value: Any) = {
    tryToPut(chunkId, value)
    true
  }
  
  def tryToPut(chunkId: String, value: Any) = {
    var chunkin: HdfsBlock = new HdfsBlock()
    chunkin.hook = value
    chunkin.length = 64*1024*1024
    bufferpool(chunkId) = chunkin
    true
  }
  
}

object MemoryChunkStore{
  
  var instance : MemoryChunkStore = null
  
  def getInstance() = {
    if(instance == null){
      instance = new MemoryChunkStore()
    }
    instance
  }
}