package storage

import java.nio.ByteBuffer
import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import common.Block._
import catalog._

/**
 * This structure maintains all the information needed to accessa block in in-memory chunk, in-disk chunk, or in-hdfs chunk.
 * The underlying reason for using this structure is to read block from Chunk.
 * For further use.
 * @author Suijun
 * 26, Aug, 2015
 */

class ChunkReaderIterator(
      private var chunkId: String,
      private var blockSize: Long,
      private var chunkSize: Long,
      private var numberOfBlocks: Long = 0){

  /*
   * block_accessor is the basic class to access block.
   * */
	class block_accessor{
    
    //virtual
		def getBlock(block:BlockStreamBase) = {}
    
    /* Set/Get function for block_size. */
		def getBlockSize():Long = {
			block_size
		}
		def setBlockSize(blockSize:Long) = {
			block_size = blockSize
		}
    
    // Size of the block
		var block_size:Long = 0L
	}
  
  /*
   * InMemeryBlockAccessor is using to access In-memory block
   * */
	class InMemeryBlockAccessor extends block_accessor{

		override def getBlock(block:BlockStreamBase) = {
    
      //Todo
      
    }
    
    /* Set/Get function for target_block. */
    def getTargetBlock():Array[Byte] = {
      target_block
    }
    def setTargetBlock(targetBlock:Array[Byte]) = {
      target_block = targetBlock
    }
    //Target block to be accessed.
    private var target_block = new Array[Byte](1024)
	}

  /*
   * InDiskBlockAccessor is using to access In-DISK block
   * */
	class InDiskBlockAccessor extends block_accessor{

		override def getBlock(block:BlockStreamBase) = {
    
      //Todo
        
    }

    /* Set/Get function for block_cur. */
		def getBlockCur():Long = {
			block_cur
		}
		def setBlockCur(blockCur:Long) =  {
			block_cur = blockCur
		}

    /* Set/Get function for chunk_id. */
		def getChunkId():String = {
			chunk_id
		}
		def setChunkId(chunkId:String) {
			chunk_id = chunkId
		}

    /* Set/Get function for chunk_size. */
		def getChunkSize():Long = {
			chunk_size
    }
		def setChunkSize(chunkSize:Long) = {
			chunk_size = chunkSize
		}

    // Size of chunk
		var chunk_size = 0L
    // Id of chunk
		var chunk_id = ""
    // Current block in chunk
		var block_cur = 0L
	}


	class InHDFSBlockAccessor extends block_accessor{

		override def getBlock(block:BlockStreamBase) = {
    
      //Todo
        
    }

    /* Set/Get function for block_cur. */
		def getBlockCur():Long = {
      block_cur
    }
    def setBlockCur(blockCur:Long) =  {
      block_cur = blockCur
    }
    
    /* Set/Get function for chunk_id. */
		def getChunkId():String = {
      chunk_id
    }
    def setChunkId(chunkId:String) {
      chunk_id = chunkId
    }

    /* Set/Get function for chunk_size. */
    def getChunkSize():Long = {
      chunk_size
    }
    def setChunkSize(chunkSize:Long) = {
      chunk_size = chunkSize
    }

    // Size of chunk
    var chunk_size = 0L
    // Id of chunk
    var chunk_id = ""
    // Current block in chunk
    var block_cur = 0L
	}



	def nextBlock():Block = null//virtual
	def getNextBlockAccessor():block_accessor = null//virtual
  
  /*
   * Below are the variable of ChunkReaderIterator
   * */
  // Id of chunl
	var chunk_id_ = chunkId
  // Number of blocks in chunk
	var number_of_blocks_ = numberOfBlocks
  // Current block index
	var cur_block_ = 0
  // Size of block
	var block_size_ = blockSize
  // Size of chunk
	var chunk_size_ = chunkSize
}


/*
 * InMemoryChunkReaderItetaor is use to read the data of the In-memory chunk.
 * Basic element of read is block.
 * */
class InMemoryChunkReaderItetaor(
      private var chunk_buffer:Array[Byte],
      private var chunkId:String,
      private var blockSize:Long,
      private var chunkSize:Long,
      private var numberOfBlocks:Long = 0) extends ChunkReaderIterator(chunkId,blockSize,chunkSize,numberOfBlocks){
	
  /*
   * Get next block of this chunk
   * */
  override def nextBlock():Block = {
   if(cur_block_ >= number_of_blocks_){
     //lock_.release();
     null
   }
   cur_block_ +=1
   var value = new Array[Byte](block_size_.toInt)
   chunk_buffer_.drop((cur_block_ * block_size_).toInt).copyToArray(value, 0, block_size_.toInt)
   var temBlock = new Block(block_size_, false, value)
   temBlock
  }
  
  /*
   * Construct a block_accessor to read the data of the block.
   * */
	override def getNextBlockAccessor():block_accessor = {
    if(cur_block_ >= number_of_blocks_){
      
      //Todo
      
      null
    }
    var cur_block = cur_block_
    cur_block_ += 1
    var imba = new InMemeryBlockAccessor()
    imba.setBlockSize(block_size_)
    var value = new Array[Byte](block_size_.toInt)
    chunk_buffer_.drop((cur_block_ * block_size_).toInt).copyToArray(value, 0, block_size_.toInt)
    imba.setTargetBlock(value)
    imba
  }

  // Data of the chunk
	var chunk_buffer_ = chunk_buffer
}

/*
 * DiskChunkReaderIteraror is use to read the data of the In-Disk chunk.
 * Basic element of read is block.
 * */
class DiskChunkReaderIteraror(
      private var chunkId:String,
      private var blockSize:Long,
      private var chunkSize:Long) extends ChunkReaderIterator(chunkId,blockSize,chunkSize){

	override def nextBlock():Block = {
    
    // Todo
    
    null
  }
  
  
  override def getNextBlockAccessor():block_accessor = {
    
    // Todo
    
    null
  }

	/*
   * The iterator creates a buffer and allocates its memory such that the query processing
	 * can just use the Block without the concern the memory allocation and deallocation.
	 */
  
  // Data of the chunk
	var block_buffer_ = new ArrayBuffer[Block]()
	// Id of the file.
  var fd_ = 0  

}

/*
 * DiskChunkReaderIteraror is use to read the data of the In-Disk chunk.
 * Basic element of read is block.
 * */
class HDFSChunkReaderIterator(
      private var chunkId:String,
      private var blockSize:Long,
      private var chunkSize:Long) extends ChunkReaderIterator(chunkId,blockSize,chunkSize){
  
	override def nextBlock():Block = {
    
    // Todo
    
    null
  }
  
  
  override def getNextBlockAccessor():block_accessor = {
    
    // Todo
    
    null
  }

	/*
   * the iterator creates a buffer and allocates its memory such that the query processing
	 * can just use the Block without the concern the memory allocation and deallocation.
	 */
 
  // Data of the chunk
	var block_buffer_ = new ArrayBuffer[Block]()

}


/*
 * According to the StorageLevel to create ChunkReaderIterator.
 * Maintain the information of chunk during the read/write.
 * */
class ChunkStorage(
    private var chunk_id:String,
    private var block_size:Long,
    private var desirable_level:StorageLevel){

/*
 * According to the StorageLevel to create ChunkReaderIterator.
 * */
	def createChunkReaderIterator():ChunkReaderIterator = {
    
    var ret: ChunkReaderIterator = new ChunkReaderIterator(null,0,0,0)
    def m(n:StorageLevel) = {
      n match {
        case StorageLevel.MEMORY_ONLY => {
          def chunk_info = MemoryChunkStore.getInstance().getChunk(chunk_id_)
          ret =new InMemoryChunkReaderItetaor(chunk_info.value_,chunk_id_,block_size_,chunk_size_,chunk_info.chunk_size_ / block_size_)
          println("Creat ChunkReaderIterator\n")
        }
        case StorageLevel.DISK_ONLY => System.out.println("Currently, current storage level should not be DISK~! -_-\n")
        case StorageLevel.HDFS_ONLY => {  System.out.println("Currently, current storage level should not be HDFS~! -_-\n")
          
        }
        case _ => System.out.println("current storage level: unknown!\n")
      }
    }
  m(desirable_level)
  return ret
  }
  
	def printCurrentStorageLevel() = {
   
    //Todo
    
    ""
  }

  /* Obtain chunk id */
	def getChunkID():String = {
    chunk_id_
  }
  
  /* Set current storage level_ */
	def setCurrentStorageLevel(current_level:StorageLevel) = {
    current_storage_level_ = current_level 
  }

  /*
   * Below are the variable of ChunkStorage.
   * */
  
  // Size of Block
	var block_size_ = block_size
  // Size of chunk.
	var chunk_size_ = 64*1024*1024L
  // Desirable storage level of chunk.
	var desirable_storage_level_ = desirable_level
  // Current storage level
	var current_storage_level_ = StorageLevel.HDFS_ONLY
  // Id of chunk
	var chunk_id_ = chunk_id
}