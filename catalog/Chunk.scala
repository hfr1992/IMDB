package catalog

import storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import java.util._

/**
 * Structure of Chunk, there four way to construct a Chunk: new Chunk(Long,String,Array[Byte],StorageLevel),
 *                                                          new Chunk(Long,String,StorageLevel),
 *                                                          new Chunk(Long,String),
 *                                                          new Chunk(Chunk)
 * Chunk is the basic container of data, it must have a unique id and a certain StorageLevel, as well as data and size.
 * It maintain the data and provide method to read or write data.
 * @author Suijun
 * 26, Aug, 2015
 */


class Chunk (
  private var chunk_size:Long,
  private var chunk_id:String,
  private var value:Array[Byte],
  private var storage_level:StorageLevel){
  
  /*
   * Constructor of Chunk.
   * In this situation,value will be initialized to be a empty Array[Byte] 
   * */
  def this(chunk_size:Long,chunk_id:String,storage_level:StorageLevel) = {
    this(chunk_size,chunk_id,new Array[Byte](chunk_size.toInt),storage_level)
  }
  
  /*
   * Constructor of Chunk.
   * In this situation,value will be initialized as empty Array[Byte] 
   * and StorageLevel will be initialized as DESIRIABLE_STORAGE_LEVEL
   * */
  def this(chunk_size:Long,chunk_id:String) = {
    this(chunk_size,chunk_id,new Array[Byte](chunk_size.toInt),StorageLevel.DESIRIABLE_STORAGE_LEVEL)
  }

  /*
   * Construct Chunk from another Chunk
   * This function will copy all the variable from the given Chunk.
   * */
  def this(chunk:Chunk) = {
    this(chunk.chunk_size_,chunk.chunk_id_,chunk.value_,chunk.storageLevel_)
  }
  
  /*
   * Put a buffer of Byte into value, and correct the actual_size.
   * */
  def putValue(valuebuffer:Array[Byte])= {
    for(i <- valuebuffer) {
      value(actual_size_.toInt) = i
      actual_size_ += 1
    }
  }
  
  /*
   * Below are the variable of Chunk. 
   * */
  // Size of Chunk
  var chunk_size_ = chunk_size
  // Unique id of Chunk
  var chunk_id_ = chunk_id
  // StorageLevel of Chunk
  var storageLevel_ = storage_level
  // Data of Chunk
  var value_ = value
  // Actual size of size
  var actual_size_ = 0L

}
