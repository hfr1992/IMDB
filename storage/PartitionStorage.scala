package storage

import java.nio.ByteBuffer
import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import common.ids._
import common.Block.Block

/**
 * PartitionStorage is use to read Chunk/Block from Partition, it has to class inside: PartitionReaderItetaor and AtomicPartitionReaderIterator
 * It manager all the chunk in a certain Partition, and provide API to access it.
 * 
 * Note that this class is current unused, just for further use..
 * @author Suijun
 * 28, Aug, 2015
 */
class PartitionStorage (
    private var partition_id:PartitionID,
    private var number_of_chunks:Long,
    private var storage_level:StorageLevel) {
  
  class PartitionReaderItetaor(private var partition_storage:PartitionStorage){
    
    def nextChunk():ChunkReaderIterator = {
      if(chunk_cur_ < ps.number_of_chunks_) {
        chunk_cur_ += 1
        ps.chunk_list_(chunk_cur_.toInt +1).createChunkReaderIterator()
      }else
        null
    }
    
    def nextBlock() :Block = {
      chunk_it_ .nextBlock()
    }
 
    var ps = partition_storage
    var chunk_cur_ = 0L
    var chunk_it_ :ChunkReaderIterator = null
  }
  
  class AtomicPartitionReaderIterator(private var partition_storage:PartitionStorage) extends PartitionReaderItetaor(partition_storage){
    
    override def nextChunk():ChunkReaderIterator = {
      if(chunk_cur_ < ps.number_of_chunks_) {
        chunk_cur_ += 1
        ps.chunk_list_(chunk_cur_.toInt +1).createChunkReaderIterator()
      }else
        null
    }
    
    override def nextBlock() :Block = {
      chunk_it_ .getNextBlockAccessor().getBlock()
    }
  }


  /*
   * Function of PartitionStorage
   * */
  def addNewChunk() = {
    number_of_chunks_ += 1
  }
  def updateChunksWithInsertOrAppend(partition_id:PartitionID,number_of_chunks:Long,storage_level:StorageLevel) = {
    
  }
  def removeAllChunks(partition_id:PartitionID) = {
    chunk_list_.clear()
    number_of_chunks_ = 0L
  }
  def createReaderIterator():PartitionReaderItetaor = {
    new PartitionReaderItetaor(this)
  }
  def createAtomicReaderIterator():PartitionReaderItetaor = {
    new AtomicPartitionReaderIterator(this)
  }

  var partition_id_ = partition_id
  var number_of_chunks_ = number_of_chunks
  var chunk_list_ :ArrayBuffer[ChunkStorage] = {
    var tmp:ArrayBuffer[ChunkStorage] = null
    for(i <- 0 to number_of_chunks_.toInt) 
      tmp += new ChunkStorage(partition_id.toString()+i.toString(),256*1024,storage_level)
    tmp
  }

  var desirable_storage_level_ = storage_level
}