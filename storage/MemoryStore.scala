package storage

import java.nio.ByteBuffer
import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import common.Block._
import catalog._

/**
 * Manage the In-memory chunk and In-memory block 
 * Provide method to put block into chunk.
 * Provide API to manage the In-memory chunk and In-memory block.
 * @author Suijun
 * 26, Aug, 2015
 */


 class MemoryChunkStore {

  /*
   * API of put data into block
   * */
	def putValue(blockId:String,value:Array[Byte]) = {
   
    // Todo
    
    tryToPut(blockId,value)
  }

  /*
   * Apply a chunk, add it into chunk_list_
   * */
	def applyChunk(chunkId:String,chunk_buf:Chunk):Boolean ={
		if(chunk_list_.contains(chunkId)){
			System.out.println("chunk id already exists!")
			false
		}else{
			chunk_list_(chunkId) = chunk_buf
			true
		}
	}

  /*
   * Return a chunk from chunk_list_ according to the chunkID.
   * Release the memory space of the chunk after return.
   * For further use
   * */
	def returnChunk(chunkId:String) = {
		if(!chunk_list_.contains(chunkId)){
			System.out.println("return fail to find the target chunk id !\n")
		}
		def chunk_info = chunk_list_.get(chunkId)
		chunk_list_.-(chunkId)
    
		// Judge whether the memory is enough
    // ...
	}

  /*
   * Update the information of chunk.
   * */
	def updateChunkInfo(chunkId:String, chunk_info:Chunk):Boolean = {
		if(!chunk_list_.contains(chunkId)){
			false
		} else {
     chunk_list_(chunkId) = chunk_info
     true 
    }
	}
  
  /*
   * Get a chunk from chunk_list_ according to the chunkID.
   * */
  def getChunk(chunkId:String) = {
    if(chunk_list_.contains(chunkId)){
        chunk_list_.get(chunkId).get
    }else null
  }

  /*
   * Add a chunk into chunk_list_
   * */
	def putChunk(chunkId:String, chunk_info:Chunk):Boolean = {
		if(chunk_list_.contains(chunkId)){
			System.out.println("The memory chunk is already existed!\n")
			return false
		}
		chunk_list_(chunkId) = chunk_info
		return true
	}

	def remove(blockId:String):Boolean = {
    
    // Todo
    
    true
  }

	def contains(blockId:String):Boolean = {
    
    // Todo
    
    false
  }

	def getSize(blockId:String):Long = {
    
    // Todo
    
    0L
  }

	/*
	 * Put the block in to buffer_pool
	 * */
	def tryToPut(blockId:String,value:Array[Byte]) = if(ensureFreeSpace){
			var blockin = new Block(1024,false,value)
			bufferpool_(blockId) = blockin
		}

  /*
   * Judge whether the memory space is enough.
   * */
	def ensureFreeSpace():Boolean = {
    
    // Todo
    
    true
  }

	/*
   * Read file from Disk
   * */
	def getFileLocation(partition_file_name:String): ArrayBuffer[String] = {
		var block_set = new ArrayBuffer[String]()
		block_set.append("/home/casa/storage/data/1")
		block_set.append("/home/casa/storage/data/2")
		block_set
	}


  // Buffer pool of In-memory block
	private var bufferpool_ = new HashMap[String,Block]()
  // List of In-memory chunk.
 	private var chunk_list_ = new HashMap[String,Chunk]()
	// Max momery of of a single node(mb), for further use.
	private var maxMemory = 0L
	// Current memory useage(MB), for further use.
	private var currentMemory = 0L
}

/*
 * Singleton object of MemoryChunkStore
 * */
object MemoryChunkStore{
	var instance : MemoryChunkStore = null
  
  /* Use getInstance to get the instance of MemoryChunkStore. */
  def getInstance() = {
    if(instance == null){
      instance = new MemoryChunkStore()
    }
    instance
  }
}