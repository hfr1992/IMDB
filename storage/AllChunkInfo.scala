/**
 * Info of Chunk and ChunkID
 * 10 Aug,2015
 * @author Suijun_524457
 */

package storage

import java.io.{ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

/*
 * Info of ChunkID
 * @tableId is the id of the table
 * @chunkOff is the offset in a certain table
 */
class ChunkID(chunk_offset:Int,table_id:String){
  
  var chunkOff = chunk_offset

  var tableId = table_id

  def ==(r:ChunkID): Boolean = this.tableId==r.tableId&&this.chunkOff==r.chunkOff
}

/*
 * common info of Chunk
 */
class ChunkInfo private(
	private var chunk_id:ChunkID,
	private var storage_level:StorageLevel){

	def chunkID = chunk_id
	def storageLevel = storage_level

}

/*
 * store all the chunk in the system
 */
object AllChunkInfo {
	var chunkList = new ArrayBuffer[ChunkID]()

	
 }