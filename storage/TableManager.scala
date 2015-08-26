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
import common.Block.BlockStreamBase
import common.Block.BlockStreamVar
import common.Block.DynamicBlockBuffer
import common.Block.PrintResultSet
import common.Schema.Schema

class TableManager {
  
  /*
   * Load RMDBS data into chunk
   * table: table of data
   * rs: resultset from RDBMS
   * */
  def loadDataToChunk(table:Table,rs:ResultSet) = {
    while(rs.next()){   // Get the record one by one
      var tuple_buf = ""
      var tuple_key = rs.getString(table.getIndexAttribute()(0).getAttrName())
      for(i <- table.getAttributeList()) {
        /* copy the data of record into tuple_buf */
        for(j <- rs.getString(i.getAttrName)) tuple_buf += j;
        tuple_buf += '^'
      }
      tuple_buf += '~'
      
      // Insert one tuple of data at one time.
      table.insertOneTuple(tuple_key, tuple_buf.getBytes)
    }
    
  }
  
  /*
   * Creat table and add it to table_list_.
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
    val dbb = new DynamicBlockBuffer()
    var block_buf = new Block(1024*1024)
    var point = 0L
    for(i <- record_position_list) {  
      //For each position, get one record
      
      if(getOneTuple(i).length + point > block_buf.BlockSize) {
        
        /* When the block is full add it to DynamicBlockBuffer, and create a new block. */
        var block = new Block(block_buf)
        dbb.atomicAppendNewBlock(new BlockStreamVar(block, new Schema()))
        block_buf.memorySpace = new Array[Byte](1024*1024)
        point = 0L
      }
      var buf = getOneTuple(i)
      
      /* Copy the data into block_buf */
      for(j <- buf) {
        block_buf.memorySpace(point.toInt) = j
        point += 1
      }
    }
    dbb.atomicAppendNewBlock(new BlockStreamVar(block_buf, new Schema()))
    
    /* Print result */
    val rs = new PrintResultSet(dbb)
    rs.printRS()
  }
  
  /*
   * Get one tuple from Chunk
   * @record_position: position of record, get from index
   * Return a Array[Byte] of data
   * */
  def getOneTuple (record_position:IndexPosition) = {
    var memoryChunk = MemoryChunkStore.getInstance().getChunk(record_position.chunk_id_)
    var start_add = record_position.tuple_offset_.toInt
    var strBuf = ""
    if (memoryChunk != null){  // Judge whether the chunk is exist.
      while(memoryChunk.value_(start_add) != '~'){
        strBuf += memoryChunk.value_(start_add).toChar
        start_add += 1
      }
      strBuf += '~'
      strBuf.getBytes
    }else {
      System.out.println("chunk don't exist!\n")
      null
    }
  }
  
  /*
   * Create table groups.
   * Just for test.
   * */
  def creatTable(table_name:String) = {
    //Below code just for test
    
    var cloumn:ArrayBuffer[Attribute] = new ArrayBuffer[Attribute]
    cloumn += new Attribute("gno","int",4)
    cloumn += new Attribute("gname","string",10)
    cloumn += new Attribute("gnum","int",4)
    cloumn += new Attribute("gintro","string",20)
    cloumn += new Attribute("cno","string",4)
    var table = new Table(table_name,cloumn)
    table
  }
  
  // List of table, Maintain all the table in this system.
  var table_list_ = new HashMap[String,Table]()
  
}

/*
 * Singleton object of TableManager
 * */
object TableManager{
  var instance : TableManager = null
  
  /* Use getInstance to get the instance of TableManager. */
  def getInstance() = {
    if(instance == null){
      instance = new TableManager()
    }
    instance
  }
}