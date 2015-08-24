package socket

import java.net.ServerSocket
import java.net.Socket
import java.io._
import java.util._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import dbConnection.DBConnection
import catalog._
import storage.TableManager
import storage.MemoryChunkStore
import sqlParser.SQLSimpleParser


/**
 * @author hfr
 */
class Server{
  
  def initializeServer() = {
    //Hint
    println("Initializing...")
    
    //Todo
    var tableManager = TableManager.getInstance()
    var memoryStore = MemoryChunkStore.getInstance()
    //println("Read from chunk"+memoryStore.+"\n")
    var groups = tableManager.creatTable("groups")
    groups.inits()
    println("Table groups is creat!")
    
    val DBCon = new DBConnection()
    val rs = DBCon.query("select * from groups;")   
    println("Loading data...\n")
    
    tableManager.loadDataToChunk(groups, rs)
    
    println("Read from chunk\n")
    tableManager.getRecordFromChunk(groups.position_list_)
//    var record = memoryStore.getChunk(groups.cur_chunk_.chunk_id_).value_
//    //var record = memoryStore.getChunk("groups_0").value_
//////    
//    //var record = groups.cur_chunk_.value_
//    var str = "" 
//    var x = 0
//    while(record(x) != 0) {str += record(x).toChar; x += 1}
//    println("DATA IS:"+str)   
    
    
  }
  
  def startListener() = {
    //Hint
    println("Starting listener...Waiting for client...")
    //start listener
    val serverListener = new ServerListener(8765)
    (new Thread(serverListener)).start()
  }
  
     def processSQL(query: String): String = {
    //Todo
    val ssp = new SQLSimpleParser()
    ssp.parser(query)
    if(ssp.getParseStatus()){
      //Query type
      println("Query type: "+ssp.getQueryType())
      //Table
      println("Table: "+ssp.getTableName())
      //Result
      println("Result:")
      val rs = ssp.getResult()
      for(x<-rs){
        for(xx<-x){
          print(xx)
        }
        println("")
      }
      
      println("")
    }
    "\"This sentence pretends to be a result.\""
  }
  
  
  class ServerListener(port: Int) extends Runnable{
    val serverSocket = new ServerSocket(port)
    val socket = serverSocket.accept()
    val inStream = new DataInputStream(socket.getInputStream())
    val outStream = new DataOutputStream(socket.getOutputStream())
    
    def run(){
      println("Successfully connected. Waiting for a query...")
      returnInfo("Successfully connected. \nPlease input query (input 'quit' to terminate the database): ")
      while(true){
        var line = inStream.readUTF()
        //Quit the program
        if(line=="quit"){
          return
        }
        println("Received command: '"+line+"'. Processing...")
        returnInfo("Query successfully. Query Result: \n----------\n"+processSQL(line)+"\n----------\n")
      }
    }
    
    def returnInfo(info: String) = {
      outStream.writeUTF(info);
    }
  }
  
}

object Server{
  def main(args: Array[String]){
    println("Starting...")
    val server = new Server()
    //initialize the resource, including loading data into the database
    server.initializeServer()
    //start the listener for SQL query
    server.startListener()
  }
}