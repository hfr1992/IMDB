/**
 * Basic class of store a chunk
 * 9 Aug,2015
 * @author Suijun_524457
 */

package storage

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import catalog._

/**
 * Abstract class to store Chunk.
 */
private abstract class ChunkStore() {
  
  def storeChunk(chunk_id:String,value:Array[Byte],length:Long=1024):Boolean = false //virtual
  //or: def storeChunk(chunk_id:ChunkID,value:ArrayBuffer[Block],length:Long=1024):Boolean = false
  
  def getChunk(chunk_id:String): ChunkStore = this  //virtual

  def remove(chunk_id:String): Boolean = false  //virtual
  def contains(chunk_id:String): Boolean = false //virtual
  def getSize(chunk_id:String): Long = 0   //Virtual
}
