package catalog

import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import storage.MemoryChunkStore

/**
 * Data structure of table, maintain the detail information of a RMDB table and some function to read/write the data of the table.
 * Use new Table(String,ArrayBuffer[Attribute]) to construct a Table.
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
}