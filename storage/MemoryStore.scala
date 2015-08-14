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
  
  class HdfsBlock(p_add: Array[Char], p_length: Int){
    var hook: Array[Char] = p_add
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
  
  def putValue(chunkId: String, value: Array[Char]) = {
    tryToPut(chunkId, value)
    true
  }
  
  def tryToPut(chunkId: String, value: Array[Char]) = {
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