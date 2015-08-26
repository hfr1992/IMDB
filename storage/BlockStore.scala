package storage

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import catalog._

/**
 * Abstract class to store Chunk.
 * For further use.
 * @author Suijun
 * 26, Aug, 2015
 */

private abstract class ChunkStore() {
  
  def storeChunk(chunk_id:String,value:Array[Byte],length:Long=1024):Boolean = false //virtual
  def getChunk(chunk_id:String): ChunkStore = this  //virtual
  def remove(chunk_id:String): Boolean = false  //virtual
  def contains(chunk_id:String): Boolean = false //virtual
  def getSize(chunk_id:String): Long = 0   //Virtual
}
