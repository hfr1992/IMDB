/**
 * Manage the InMemory chunk 
 * Leave out the lock for the time being
 * 10 Aug,2015
 * @author Suijun_524457
 */

package storage

import java.nio.ByteBuffer
import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import common.Block._


/*
 * DataStructure of InMemoryChunk
 * @blockPool is the block pool
 * @length is the size of a single block
 * */
class HdfsBlock(
	private var add:Array[Byte],
	private var block_length:Long){
	var hook = add
	var length  = block_length
}

 

 class MemoryChunkStore {
   
   type HdfsInMemoryChunk = HdfsBlock

	def putValue(blockId:String,value:Array[Byte]) = tryToPut(blockId,value)

	def applyChunk(chunkId:ChunkID,block_buf:Array[Byte]):Boolean ={
		if(chunk_list_.contains(chunkId)){
			System.out.println("chunk id already exists!")
			false
		}/*else if(!BufferManager::getInstance()->applyStorageDedget(CHUNK_SIZE)){
			System.out.println("not enough memory!!\n")
		}*/else{
			chunk_list_(chunkId) = new HdfsInMemoryChunk(block_buf,64*1024)
			true
		}
	}

	def returnChunk(chunkId:ChunkID) = {
		if(!chunk_list_.contains(chunkId)){
			System.out.println("return fail to find the target chunk id !\n")
		}
		def chunk_info = chunk_list_.get(chunkId)
		chunk_list_.-(chunkId)
		//BufferManager::getInstance()->returnStorageBudget(chunk_info.length)
    //@Waiting for solve
	}

	def updateChunkInfo(chunkId:ChunkID, chunk_info:HdfsInMemoryChunk):Boolean = {
		//lock_.acquire();
		if(!chunk_list_.contains(chunkId)){
			//lock_.release();
			return false
		}
		chunk_list_(chunkId) = chunk_info
		//lock_.release();
		return true
	}

  /*get block value from */
	def getChunk(blockId:String):Array[Byte]={
		if(bufferpool_.contains(blockId)){
        bufferpool_.get(blockId).get.hook
    }else null
    //lock_.release();		
	}
  
  /*get chunk from chunk_list_*/
  def getChunk(chunkId:ChunkID):Option[HdfsInMemoryChunk] = {
    //lock_.acquire();
    if(chunk_list_.contains(chunkId)){
        chunk_list_.get(chunkId)
    }else null
    //lock_.release();    
  }

	def putChunk(chunkId:ChunkID, chunk_info:HdfsInMemoryChunk):Boolean = {
		//lock_.acquire();
		if(chunk_list_.contains(chunkId)){
			System.out.println("The memory chunk is already existed!\n")
			//lock_.release();
			return false
		}
		chunk_list_(chunkId) = chunk_info
		//lock_.release();
		return true
	}

	def remove(blockId:String):Boolean = true

	def contains(blockId:String):Boolean = false

	def getSize(blockId:String):Long = 0

	/*
	 * put the block in to buffer_pool
	 * */
	def tryToPut(blockId:String,value:Array[Byte]) = if(ensureFreeSpace){
			var chunkin = new HdfsBlock(value,1024)
			bufferpool_(blockId) = chunkin
		}

	def ensureFreeSpace():Boolean = true

	
	def getFileLocation(partition_file_name:String): ArrayBuffer[String] = {
		var block_set = new ArrayBuffer[String]()
		block_set.append("/home/casa/storage/data/1")
		block_set.append("/home/casa/storage/data/2")
		block_set
	}


	private var bufferpool_ = new HashMap[String,HdfsBlock]()
	private var chunk_list_ = new HashMap[ChunkID,HdfsInMemoryChunk]()
	// Max momery of of a single node(mb)
	private var maxMemory = 0L
	// Current memory useage(MB)
	private var currentMemory = 0L

	private var chunk_pool_ =  new ArrayBuffer[HdfsInMemoryChunk]()
	private var block_pool_ = new ArrayBuffer[HdfsBlock]()

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