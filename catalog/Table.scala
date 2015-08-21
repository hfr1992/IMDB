package catalog

/**
 * @author Suijun
 */

import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import storage.MemoryChunkStore



class Table(
    private var table_name:String,
    private var attribute_list:ArrayBuffer[Attribute]) {
 /*
  * Set-Get function for index_attribute_
  * @ Can only set one attribute into index_attribute_ at one time
  * */
  
  def inits(){
    setIndexAttribute(attribute_list_(0))
    MemoryChunkStore.getInstance().applyChunk(cur_chunk_.chunk_id_, cur_chunk_)
  }
  
  def setIndexAttribute(attr:Attribute) = {
    index_attribute_ += attr
  }
  
  def getIndexAttribute() = {
    index_attribute_ 
  }
  
   /*
  * Set-Get function for attribute_list_
  * @ Can only set one attribute into attribute_list_ at one time
  * */
  def setAttributeList(attr:Attribute) = {
    attribute_list_ += attr
  }
  
  def getAttributeList() = {
    attribute_list_ 
  }
  
  /*
   * Set-Get function for chunk_num_
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
    var tmpPair = key_position_pair_
    key_position_pair_.clear()
    tmpPair
  }
  
//  def loadDataToChunk(value:ArrayBuffer[Byte]) = {
//    var cur_point = 0
//    
//    while(cur_point < value.length){
//      var tmpArr = new ArrayBuffer[Byte]
//      for(i <- cur_point until cur_point+record_length_)
//        tmpArr += value(i)
//      if((cur_chunk_.actual_size_ + record_length_) > cur_chunk_.chunk_size_){
//        //MemoryChunkStore.getInstance().applyChunk(cur_chunk_.chunk_id_, cur_chunk_)
//        cur_chunk_ = new Chunk(64*1024*1024,new ChunkID(chunk_num_,table_name_))
//        chunk_num_ += 1
//      }else //insertOneTuple(tmpArr)
//      cur_point += record_length_
//    }
//    
//  }
  
  def insertOneTuple(tuple_key:String, tuple_value:Array[Byte]) = {
    //println("befo insert"+tuple_value.length + "\n")
      if((cur_chunk_.actual_size_ + tuple_value.length) > cur_chunk_.chunk_size_){
        cur_chunk_ = new Chunk(64*1024*1024,new ChunkID(chunk_num_,table_name_))
        MemoryChunkStore.getInstance().applyChunk(cur_chunk_.chunk_id_, cur_chunk_)
        chunk_num_ += 1
      }
     cur_chunk_.putValue(tuple_value)
     var position = new IndexPosition(cur_chunk_.chunk_id_,cur_chunk_.actual_size_ - tuple_value.length)
     key_position_pair_(tuple_key) = position
  }
  
  //Name of the table
  var table_name_ = table_name
  
  //Size of each record
  private var record_length_ = {
    var length = 0
    for(i <- attribute_list)
      length += i.getAttrLength()
    length
  }
  
  //List of attribute
  private var attribute_list_ = attribute_list
  
  //List of the index attribute
  private var index_attribute_ = new ArrayBuffer[Attribute]()
  
  //Amount of chunk
  private var chunk_num_ = 1
  
  //Key-Position pair of index for a certain attribute
  var key_position_pair_ = new HashMap[String,IndexPosition]()
  
  //key-Position pair for all the index attribute
  //@For further use
  var kp_pair_list_ = new HashMap[Attribute,HashMap[String,IndexPosition]]
  
  var cur_chunk_ = new Chunk(64*1024*1024,new ChunkID(0,table_name_))
}