/**
 * ChunkStorage module
 * 10 Aug,2015
 * @author Suijun_524457
 */

package storage

import java.nio.ByteBuffer
import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import common.Block._
import catalog._


class ChunkReaderIterator(
      private var chunkId: String,
      private var blockSize: Long,
      private var chunkSize: Long,
      private var numberOfBlocks: Long = 0){
  
	/**
	 * This structure maintains all the information needed to access
	 *  a block in in-memory chunk, in-disk chunk, or in-hdfs chunk.
	 *
	 *  The underlying reason for using this structure is to.
	 */
  
	class block_accessor{
    
		def getBlock(block:BlockStreamBase) = {}
    
		def getBlockSize():Long = {
			block_size
		}

		def setBlockSize(blockSize:Long) = {
			block_size = blockSize
		}
    
		var block_size:Long = 0L
	}
  
	class InMemeryBlockAccessor extends block_accessor{

		override def getBlock(block:BlockStreamBase) = {}
    
    def getTargetBlock():Array[Byte] = {
      target_block
    }
    
    def setTargetBlock(targetBlock:Array[Byte]) = {
      target_block = targetBlock
    }
    
    private var target_block = new Array[Byte](1024)
	}

	class InDiskBlockAccessor extends block_accessor{

		override def getBlock(block:BlockStreamBase) = {}

		def getBlockCur():Long = {
			block_cur
		}

		def setBlockCur(blockCur:Long) =  {
			block_cur = blockCur
		}

		def getChunkId():String = {
			chunk_id
		}

		def setChunkId(chunkId:String) {
			chunk_id = chunkId
		}

		def getChunkSize():Long = {
			chunk_size
    }

		def setChunkSize(chunkSize:Long) = {
			chunk_size = chunkSize
		}

		var chunk_size = 0L
		var chunk_id = ""
		var block_cur = 0L
	}


	class InHDFSBlockAccessor extends block_accessor{

		override def getBlock(block:BlockStreamBase) = {}

		def getBlockCur():Long = {
      block_cur
    }

    def setBlockCur(blockCur:Long) =  {
      block_cur = blockCur
    }
    
		def getChunkId():String = {
      chunk_id
    }

    def setChunkId(chunkId:String) {
      chunk_id = chunkId
    }

    def getChunkSize():Long = {
      chunk_size
    }

    def setChunkSize(chunkSize:Long) = {
      chunk_size = chunkSize
    }

    var chunk_size = 0L
    var chunk_id = ""
    var block_cur = 0L
	}



	def nextBlock():Block = null//virtual
  
	def getNextBlockAccessor():block_accessor = null//virtual
  
	var chunk_id_ = chunkId
	var number_of_blocks_ = numberOfBlocks
	var cur_block_ = 0
	//var lock_:Lock
	var block_size_ = blockSize
	var chunk_size_ = chunkSize
}


class InMemoryChunkReaderItetaor(
      private var chunk_buffer:Array[Byte],
      private var chunkId:String,
      private var blockSize:Long,
      private var chunkSize:Long,
      private var numberOfBlocks:Long = 0) extends ChunkReaderIterator(chunkId,blockSize,chunkSize,numberOfBlocks){
	
  
  /*@ modify BlockStreamBase into Block*/
  override def nextBlock():Block = {
    //lock_.acquire();
    if(cur_block_ >= number_of_blocks_){
      //lock_.release();
      null
    }
    cur_block_ +=1
   var value = new Array[Byte](block_size_.toInt)
   chunk_buffer_.drop((cur_block_ * block_size_).toInt).copyToArray(value, 0, block_size_.toInt)
   var temBlock = new Block(block_size_, false, value)
    temBlock
    //hdfs_in_memory_chunk_.blockPool(cur_block_)
    
  }
	override def getNextBlockAccessor():block_accessor = {
   //lock_.acquire()
    if(cur_block_ >= number_of_blocks_){
      //lock_.release()
      null
    }
    var cur_block = cur_block_
    cur_block_ += 1
    //lock_.release()
    var imba = new InMemeryBlockAccessor()
    imba.setBlockSize(block_size_)
    var value = new Array[Byte](block_size_.toInt)
    chunk_buffer_.drop((cur_block_ * block_size_).toInt).copyToArray(value, 0, block_size_.toInt)
    imba.setTargetBlock(value)
    imba
  }

	var chunk_buffer_ = chunk_buffer

}

class DiskChunkReaderIteraror(
      private var chunkId:String,
      private var blockSize:Long,
      private var chunkSize:Long) extends ChunkReaderIterator(chunkId,blockSize,chunkSize){

	override def nextBlock():Block = null
  
  override def getNextBlockAccessor():block_accessor = null

//	unsigned number_of_blocks_;
//	unsigned cur_block_;
	/*the iterator creates a buffer and allocates its memory such that the query processing
	 * can just use the Block without the concern the memory allocation and deallocation.
	 */
	var block_buffer_ = new ArrayBuffer[Block]()
	var fd_ = 0  //file id

}

class HDFSChunkReaderIterator(
      private var chunkId:String,
      private var blockSize:Long,
      private var chunkSize:Long) extends ChunkReaderIterator(chunkId,blockSize,chunkSize){
  
	//virtual ~HDFSChunkReaderIterator();
	override def nextBlock():Block = null
  
  override def getNextBlockAccessor():block_accessor = null

//	unsigned number_of_blocks_;
//	unsigned cur_block_;
	/*the iterator creates a buffer and allocates its memory such that the query processing
	 * can just use the Block without the concern the memory allocation and deallocation.
	 */
	var block_buffer_ = new ArrayBuffer[Block]()
	//hdfsFS fs_;
	//hdfsFile hdfs_fd_;

};

class ChunkStorage(
    private var chunk_id:String,
    private var block_size:Long,
    private var desirable_level:StorageLevel){


	/* considering that how block size effects the performance is to be tested, here we leave
	 * a parameter block_size for the performance test concern.
	 */
  
	//virtual ~ChunkStorage();
	def createChunkReaderIterator():ChunkReaderIterator = {
  var ret: ChunkReaderIterator = new ChunkReaderIterator(null,0,0,0)
  //lock_.acquire();
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
  //lock_.release();
  return ret
  }
  
	def printCurrentStorageLevel() = ""

	def getChunkID():String = {chunk_id_}
	def setCurrentStorageLevel(current_level:StorageLevel) = { current_storage_level_ = current_level }

	var block_size_ = block_size
	var chunk_size_ = 64*1024L
	var desirable_storage_level_ = desirable_level
	var current_storage_level_ = StorageLevel.HDFS_ONLY
	var chunk_id_ = chunk_id

	//Lock lock_
}