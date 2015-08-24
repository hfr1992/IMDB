package storage

/**
 * @author Suijun
 */
import java.nio.ByteBuffer
import java.util._
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import catalog._
import common.Block.Block

class TableManager {
  
  /*
   * Load RMDBS data into chunk
   * @table: table of data
   * @rs: resultset from RDBMS
   * */
  def loadDataToChunk(table:Table,rs:ResultSet) = {
    //var test = 1
    while(rs.next()){
      var tuple_buf = ""
      var tuple_key = rs.getString(table.getIndexAttribute()(0).getAttrName())
      
      for(i <- table.getAttributeList()) {
        for(j <- rs.getString(i.getAttrName)) tuple_buf += j;
        tuple_buf += '^'
      }// tuple_buf.concat(rs.getString(i.getAttrName)+'^')
      tuple_buf += '~'
//      printf("tuple_buf = "+tuple_buf+"\n")
      table.insertOneTuple(tuple_key, tuple_buf.getBytes)
      
     //println(tuple_buf+"   "+ test + "\n")
      //test += 1
    }
    
  }
  
  /*
   * Creat table
   * */
  def creatTable(table_name:String,table:Table) = {
    if(table_list_.contains(table_name)){
      System.out.println("Table"+" is aready exist!")
      false
    }else table_list_(table_name) = table
  }
  
  /*
   * Get records from chunk according to the position list
   * */
  def getRecordFromChunk(record_position_list:ArrayBuffer[IndexPosition]) = {
    var block_list = new ArrayBuffer[Block]
    var block_buf = new Block(1024*1024)
    var point = 0L
    
    print("record_position_list length = "+record_position_list.length + "\n")
    
    for(i <- record_position_list) {
      if(getOneTuple(i).length + point > block_buf.BlockSize) {
        var block = new Block(block_buf)
        block_list += block
        block_buf.memorySpace = new Array[Byte](1024*1024)
        point = 0L
      }
      //print("point = "+point+"\n")
      var buf = getOneTuple(i)
      for(j <- buf) {
        block_buf.memorySpace(point.toInt) = j
        point += 1
      }
      
    }
    block_list
  }
  
  /*
   * Get one tuple from Chunk
   * @record_position: position of record, get from index
   * Return a Array[Byte] of data
   * */
  def getOneTuple (record_position:IndexPosition) = {
   // printf("get tuple \n")
    var memoryChunk = MemoryChunkStore.getInstance().getChunk(record_position.chunk_id_)
    
    var start_add = record_position.tuple_offset_.toInt
    var strBuf = ""
    if (memoryChunk != null){
      while(memoryChunk.value_(start_add) != '~'){
        strBuf += memoryChunk.value_(start_add).toChar
        start_add += 1
      }
      strBuf += '~'
      //printf("data:"+strBuf+"\n")
      strBuf.getBytes
    }else {
      System.out.println("chunk don't exist!\n")
      null
    }
  }
  
  def creatTable(table_name:String) = {
    var cloumn:ArrayBuffer[Attribute] = new ArrayBuffer[Attribute]
    cloumn += new Attribute("gno","int",4)
    cloumn += new Attribute("gname","string",10)
    cloumn += new Attribute("gnum","int",4)
    cloumn += new Attribute("gintro","string",20)
    cloumn += new Attribute("cno","string",4)
    var table = new Table(table_name,cloumn)
    table
  }
  
  var table_list_ = new HashMap[String,Table]()
  
}

object TableManager{
  var instance : TableManager = null
  
  def getInstance() = {
    if(instance == null){
      instance = new TableManager()
    }
    instance
  }
}