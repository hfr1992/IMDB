package catalog

/**
 * @author Suijun
 */

import storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

import java.util._

/*
 * Info of ChunkID
 * @tableId is the id of the table
 * @chunkOff is the offset in a certain table
 */
//class ChunkID(
//    private var chunk_offset:Int,
//    private var table_id:String){
//  
//  def this() = {
//    this(0,"")
//  }
//  var chunkOff = chunk_offset
//
//  var tableId = table_id
//
//  def ==(r:ChunkID): Boolean = this.tableId==r.tableId&&this.chunkOff==r.chunkOff
//  
//  def eqpuls(r:ChunkID) = this.tableId==r.tableId&&this.chunkOff==r.chunkOff
//}

/*
 * common info of Chunk
 */
class Chunk (
  private var chunk_size:Long,
  private var chunk_id:String,
  private var value:Array[Byte],
  private var storage_level:StorageLevel){
  
  def this(chunk_size:Long,chunk_id:String,storage_level:StorageLevel) = {
    this(chunk_size,chunk_id,new Array[Byte](chunk_size.toInt),storage_level)
  }
  
  def this(chunk_size:Long,chunk_id:String) = {
    this(chunk_size,chunk_id,new Array[Byte](chunk_size.toInt),StorageLevel.DESIRIABLE_STORAGE_LEVEL)
  }

  def this(chunk:Chunk) = {
    this(chunk.chunk_size_,chunk.chunk_id_,chunk.value_,chunk.storageLevel_)
  }
  
  def putValue(valuebuffer:Array[Byte])= {
   // var str = ""
    for(i <- valuebuffer) {
     // str += i.toChar
      value(actual_size_.toInt) = i
      actual_size_ += 1
    }
//    var str = ""
//    var i =0
//    while(value(i) != 0) {str += value(i).toChar; i += 1}
//    println("value = "+str+"\n")
  }
  
  var chunk_size_ = chunk_size
  var chunk_id_ = chunk_id
  var storageLevel_ = storage_level
  var value_ = value
  var actual_size_ = 0L

}

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
