package storage

import java.nio.ByteBuffer
import java.util._
import common.Block._


/**
 * Disk chunk store,loading data from disk into chunk(In memory)
 * or write chunk into a certain dir
 * For further use
 * @author Suijun
 * 26, Aug, 2015
 */



class DiskStore(private var rootDirs:String){
  
  /* Use blockId as the file name when the data was write to disk
   * Value is a point
   * 
   * */
  def putValue(blockId:String,value:Array[Byte]):Boolean = {
    
    //Todo
    
    true
  }

  /*load the file into Chunk
   * and return the Chunk
   * */
  def getChunk(blockId:String) = {
    
    //Todo 
    
  }
  
  def remove(blockId:String):Boolean = true
  def contains(blockId:String):Boolean = true
  def getSize(blockId:String):Long = 0L
  def getFile(blockId:String):Boolean = true

  // Creat dir for a gaven path
  def createDirs(rootDirs:String):Boolean = {
    // use rootDirs to creat a dir on disk
    
    // Todo
    
    true
}

  var rootDirs_ = rootDirs
}