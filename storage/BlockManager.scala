package storage

import common.Block._
import scala.collection.mutable.Map

/**
 * Provide API to access and manage block.
 * For further use
 * @author Suijun
 * 26, Aug, 2015
 */

class BlockManager {
  
  // Block poll
  var blockInfoPool = Map[String, Block]()
  // Singleton object of MemoryChunkStore
  var memstore = MemoryChunkStore.getInstance()
  // Singleton object of DiskStore
  var diskstore = new DiskStore("")
}

/*
 * Singleton object of StoragLevel
 * */
object BlockManager{
  var blockmanager : BlockManager = null
  
  /* Use getInstance to get the blockManager of object BlockManager */
  def getInstance = {
    if(blockmanager == null){
      blockmanager = new BlockManager()
    }
    blockmanager
  }
}