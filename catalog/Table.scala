package catalog

import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import storage.MemoryChunkStore
import common.ids._

/**
 * Data structure of table, maintain the detail information of a RMDB table and some function to read/write the data of the table.
 * Use new Table(String,ArrayBuffer[Attribute]) to construct a Table.
 * 
 * Projecttion is use for partition, and it haven't being used right now.
 * @author Suijun
 * 26, Aug, 2015
 */


class Table(
    private var table_name:String,
    private var attribute_list:ArrayBuffer[Attribute]) {
  
  /*
   * Initialize function of table.
   * Apply the current Chunk, add it to In-memory Chunk pool.
   * Set the first attribute as the index column.
   * */
  def inits(){
    setIndexAttribute(attribute_list_(0))  //by default
    MemoryChunkStore.getInstance().applyChunk(cur_chunk_.chunk_id_, cur_chunk_)
  }
  
 /*
  * Set/Get function for index_attribute_
  * Can only set one attribute into index_attribute_ at one time
  * */
  def setIndexAttribute(attr:Attribute) = {
    index_attribute_ += attr
  }
  def getIndexAttribute() = {
    index_attribute_ 
  }
  
 /*
  * Set/Get function for attribute_list_
  * Can only set one attribute into attribute_list_ at one time
  * */
  def setAttributeList(attr:Attribute) = {
    attribute_list_ += attr
  } 
  def getAttributeList() = {
    attribute_list_ 
  }
  
  /*
   * Set/Get function for chunk_num_
   * */
  def setChunkNum(chunkNum:Int) = {
    chunk_num_ = chunkNum
  }
  def getChunkNum() = {
    chunk_num_
  }
  
  /*
   * Return Key-Position pair to build index
   * */
  def returnKeyPositionPair = {
   key_position_pair_
   
   //Need to be cleaned after return the key_position_pair_
   
  }
  
  /*
   * Provide API for insert one tuple in a time.
   * tuple_key is the key to build index.
   * tuple_value is the data of a tuple.
   * */
  def insertOneTuple(tuple_key:String, tuple_value:Array[Byte]) = {
     if((cur_chunk_.actual_size_ + tuple_value.length) > cur_chunk_.chunk_size_){
       /* After a chunk is full, create a new chunk and add it to In-memory Chunk pool. */
       cur_chunk_ = new Chunk(64*256*1024,table_name_ +"_"+ (chunk_num_).toString())
       MemoryChunkStore.getInstance().applyChunk(cur_chunk_.chunk_id_, cur_chunk_)
       chunk_num_ += 1
     }
     
     cur_chunk_.putValue(tuple_value)
     
     /*
      * Below code will record the key-position pair of each tuple. In order to build index on a certain column.
      * The key-position pair will store in key_position_pair_.
      * */
     var position = new IndexPosition(cur_chunk_.chunk_id_,cur_chunk_.actual_size_ - tuple_value.length)
     position_list_ += position //for test
     var indexMap = new IndexPositionMap(tuple_key,position)
     key_position_pair_  += indexMap
  }
  
  //Size of each record
  private var record_length_ = {
    var length = 0
    for(i <- attribute_list)
      length += i.getAttrLength()
    length
  }
  
  /*
   * Below are the variable of Table
   * */
  // Name of the table
  var table_name_ = table_name
  // Current Chunk of the table, use for append data.
  var cur_chunk_ = new Chunk(64*256*1024, table_name_ +"_"+ 0.toString())
  // List of attribute
  private var attribute_list_ = attribute_list
  // List of the index attribute
  private var index_attribute_ = new ArrayBuffer[Attribute]()
  // Amount of chunk
  private var chunk_num_ = 1
  // Key-Position pair of index for a certain attribute
  var key_position_pair_ = new ArrayBuffer[IndexPositionMap]()
  
  /*
   * Position list of all the data has be inserted into this table. 
   * Use for test.
   * */ 
  var position_list_ = new ArrayBuffer[IndexPosition] 
  
  /*
   * key-Position pair for all the index attribute. 
   * For futher use.
   * */
  var kp_pair_list_ = new HashMap[Attribute,ArrayBuffer[IndexPositionMap]]
  
  /*
   * Below variable is copy from CLAIMS 
   * For further use..
   * */
  var table_id_ :TableID = 0
  var projection_list_ = new ArrayBuffer[ProjectionDescriptor]
  var row_number_ = 0L
  
  /*
   * Set/Get function of table_id_ 
   * */
  def setTableId(tid:TableID) = {
    table_id_ = tid
  }
  def getTableId() = {
    table_id_
  }
  
  /*
   * Set/Get function of projection_list_ 
   * */
  def setProjectionList(proList:ArrayBuffer[ProjectionDescriptor]) = {
    projection_list_ = proList
  }
  def getProjectionList() = {
    projection_list_
  }
  
  def createHashPartitionedProjection(column_list:ArrayBuffer[ColumnOffset],partition_key_index:ColumnOffset,number_of_partitions:Long) = {
    
    def projection_id = new ProjectionID(table_id_,projection_list_.length)
    def projection = new ProjectionDescriptor(projection_id)
    
    for (i <- column_list) 
      projection.addAttribute(attribute_list_(i.toInt))

    projection.DefinePartitonier(number_of_partitions, attribute_list_(partition_key_index.toInt))
    projection_list_ += projection
    true
  }
  
  def createHashPartitionedProjection(column_list:ArrayBuffer[ColumnOffset],partition_attribute_name:String,number_of_partitions:Long) = {
    def projection_id = new ProjectionID(table_id_,projection_list_.length)
    def projection = new ProjectionDescriptor(projection_id)

    for (i <- column_list) 
      projection.addAttribute(attribute_list_(i.toInt))

    projection.DefinePartitonier(number_of_partitions,getAttribute(partition_attribute_name))
    projection_list_ += projection
    true
  }
  
  def createHashPartitionedProjectionOnAttribute(column_list:ArrayBuffer[Attribute],partition_attribute_name:String,number_of_partitions:Long) = {
    def projection_id = new ProjectionID(table_id_,projection_list_.length)
    def projection = new ProjectionDescriptor(projection_id)

    for (i <- column_list) 
      projection.addAttribute(i)

    projection.DefinePartitonier(number_of_partitions,getAttribute(partition_attribute_name))
    projection_list_ += projection
    true
  }
  
  def createHashPartitionedProjectionOnAllAttribute(partition_attribute_name:String,number_of_partitions:Long) = {
    def projection_id = new ProjectionID(table_id_,projection_list_.length)
    def projection = new ProjectionDescriptor(projection_id)
    for (i <- attribute_list_) 
      projection.addAttribute(i)

    projection.DefinePartitonier(number_of_partitions,getAttribute(partition_attribute_name))
    projection_list_ += projection
    true
  }
  
  def getAttribute(name:String):Attribute = {
    for (i <- attribute_list_)
      if (i.getAttrName().equals(name))
        return i
    return null  
  }
}