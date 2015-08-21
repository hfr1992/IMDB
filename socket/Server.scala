package socket

import java.net.ServerSocket
import java.net.Socket
import java.io._
import sqlParser.SQLSimpleParser
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
    var cloumn:ArrayBuffer[Attribute] = new ArrayBuffer[Attribute]
    cloumn += new Attribute("gno","int",4)
    cloumn += new Attribute("gname","string",10)
    cloumn += new Attribute("gnum","int",4)
    cloumn += new Attribute("gintro","string",20)
    cloumn += new Attribute("cno","string",4)
    var groups = new Table("groups",cloumn)
    groups.inits()
    println("Table groups is creat!")
    val DBCon = new DBConnection()
    
    val rs = DBCon.query("select * from groups;")
    
//     while(rs.next()){
//      println(rs.getString("gno")+" "+rs.getString("gname")+" "+rs.getString("gnum")+" "+rs.getString("gintro")+" "+rs.getString("cno"))
//    }
//    
//    println("after insert!\n")
//    DBCon.insert("insert into groups values (101, 'hello_suijun', '1', 'Is this a group?', '11111111');")
//    
//    val rs2 = DBCon.query("select * from groups;")
//    while(rs2.next()){
//      println(rs2.getString("gno")+" "+rs2.getString("gname")+" "+rs2.getString("gnum")+" "+rs2.getString("gintro")+" "+rs2.getString("cno"))
//    }
//    
    println("Loading data...\n")
    TableManager.getInstance().loadDataToChunk(groups, rs)
    println("Read from chunk\n")
    //var record = MemoryChunkStore.getInstance().getChunk(new ChunkID(0,"groups")).value_
    var record = groups.cur_chunk_.value_
    var str = "" 
    var x = 0
    while(record(x) != 0) {str += record(x).toChar; x += 1}
    //for (i <- record) str += i.toChar;
    println("DATA IS:\n"+str)
//    
    //Todo
    
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
      println("Successfully connected. \nWaiting for a query...")
      returnInfo("Successfully connected. \nPlease input query (input 'quit' to terminate the database): ")
      while(true){
        var line = inStream.readUTF()
        //Quit the program
        if(line=="quit"){
          return
        }
        println("Received command: '"+line+"'. \nProcessing...")
        returnInfo("Query successfully. \nQuery Result: \n----------\n"+processSQL(line)+"\n----------\n")
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