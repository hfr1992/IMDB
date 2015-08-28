package common

/**
 * All the ids are defined in this file or better code organization purposes.
 * For further use.
 * @author Suijun
 * 27, Aug, 2015
 */

object ids {
 
  /*
   * Below are ids define on primary data type
   * */
  type NodeID = Int
  type TableID = Long
  
  type AttributeOffset = Long
  type ProjectionOffset = Int
  type ColumnOffset = Long
  type PartitionOffset = Long
  type ChunkOffset = Int
  type ExpanderID = Long
  
  class NodeAddress(
      private var ip:String,
      private var port:String) {
    
    def this () = {
      this("","")
    }
    
    def == (r:NodeAddress):Boolean = {
      ip_ == r.ip_ && port_ == r.port
    }
    
    override def toString() = {
      ip + "_" + port
    }
 
    var ip_ :String = ip
    var port_ :String = port
  
  }


/**
 * AttributeID: an attribute in a table has an unique AttributeID
 */
   class AttributeID (
       private var tid:TableID,
       private var off:AttributeOffset){
     
     var table_id:TableID = tid
     var offset:AttributeOffset = off
     
     def this(){
       this(0L,0L)
     }
     
     def == (r:AttributeID):Boolean = {
       table_id == r.table_id && offset == r.offset
     }
     
     override def toString() = {
      tid.toString() + "_" + off.toString()
    }
 
   } 


/**
 * ProjectionID: a projection has an unique ProjectionID
 */
   class ProjectionID (
       private var tid:TableID,
       private var off:ProjectionOffset){
     
     var table_id:TableID = tid
     var projection_off:ProjectionOffset = off
     
     def this(){
       this(0L,0)
     }
     
     def ==(r:ProjectionID):Boolean = {
       table_id == r.table_id && projection_off == r.projection_off
     }
     
     def < (r:ProjectionID):Boolean = {
      if (table_id == r.table_id)
        projection_off < r.projection_off
    else
        false
     }
     
     override def toString() = {
      tid.toString() + "_" + off.toString()
    }
   }

/**
 * ColumnID: a Column corresponds to an attribute and is physically stored in one projection.
 */
   class ColumnID(
       private var pid:ProjectionID,
       private var off:ColumnOffset) {
     
     var projection_id = pid
     var column_off = off
     
     def this() = {
       this(new ProjectionID(),0L)
     }
     
     def == (r:ColumnID):Boolean = {
       projection_id == r.projection_id && column_off == r.column_off
     }
     
     override def toString() = {
      pid.toString() + "_" + off.toString()
    }
   }

/**
 * PartitionID: a partition corresponds to one projection.
 */
   class PartitionID(
       private var projection_id:ProjectionID,
       private var off:PartitionOffset) {
     
     var projection_id_ = projection_id
     var partition_off = off
     
      def this() {
       this(new ProjectionID(),0L)
     }
     
     def this(r:PartitionID) {
       this(r.projection_id,r.partition_off)
     }
     
     def == (r:PartitionID):Boolean = {
       projection_id_ == r.projection_id_ && partition_off == r.partition_off
     }
     
     def getName():String ={
       var name = ""
       name += 'T'
       name += projection_id_.table_id.toChar
       name += 'G'
       name += projection_id_.projection_off.toChar
       name += partition_off.toChar
       name
     }
     
     def getPathAndName():String = {
       ""
     }
     
     override def toString() = {
      projection_id.toString() + "_" + off.toString()
    }
   }
/**
 * ChunkID: a chunk corresponds to one partition.
 */
   class ChunkID(
       private var partition_id:PartitionID,
       private var chunk_off:ChunkOffset) {
     
     var partition_id_ = partition_id
     var chunk_off_ = chunk_off
     
     def this(r:ChunkID) {
       this(r.partition_id_,r.chunk_off_)
     }
     
     def == (r:ChunkID):Boolean = {
       partition_id_ == r.partition_id_ && chunk_off_ ==r.chunk_off_
     }
     
     def < (r:ChunkID):Boolean = {
       if (partition_id == r.partition_id)
           chunk_off_ < r.chunk_off_
    else
       false
     }
     
     override def toString() = {
      partition_id.toString() + "_" + chunk_off.toString()
     }
  }

 /*
  * ExchangeID is design for chunk exchange
  * */
   class ExchangeID (
       private var partition_offset:Long,
       private var exchange_id:Long) {
     
     var partition_offset_ = partition_offset
     var exchange_id_ = exchange_id
     
     def this () {
       this(0L,0L)
     }
     
     def ==(r:ExchangeID):Boolean = {
       exchange_id_ == r.exchange_id_ && partition_offset_ == r.partition_offset_
     }
     
     override def toString() = {
      partition_offset.toString() + "_" + exchange_id.toString()
     }
   }
}