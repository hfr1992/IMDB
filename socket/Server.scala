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
import common.Schema.Schema
import common.Block.BlockStreamVar
import common.Block.DynamicBlockBuffer

/**
 * @author Feiran
 * The Server is the class initializing the resource, getting the SQL query from the client, processing it and printing the result.
 * It create a thread to listen the Client.
 */
class Server{
  
  
  // Object of TableManager
  var tableManager = TableManager.getInstance()
  // Object of MemoryStore
  var memoryStore = MemoryChunkStore.getInstance()

  /**
   * Initializing the server, as well as the resource.
   * Loading data from PostgreSQL.
   */
  def initializeServer() = {
    //Hint
    println("Initializing...")
    
    
    //println("Read from chunk"+memoryStore.+"\n")
    var groups = tableManager.creatTable("groups")
    groups.inits()
    println("Table groups is creat!")
    
    val DBCon = new DBConnection()
    val rs = DBCon.query("select * from groups;")   
    println("Loading data...\n")
    
    tableManager.loadDataToChunk(groups, rs)
    
//    println("Read from chunk\n")
//    tableManager.getRecordFromChunk(groups.position_list_)
  }
  
  /**
   * Start the server listener.
   */
  def startListener() = {
    //Hint
    println("Starting listener...Waiting for client...")
    //start listener
    val serverListener = new ServerListener(8765)
    (new Thread(serverListener)).start()
  }
  
 /*
  * process the query according to the query type
  * */
  def parserQuery(typeOfQuery:String,tableOfQuery:String,resultSet:ArrayBuffer[Array[String]]) = {
    typeOfQuery match{
      case "insert" => {
         var table = tableManager.table_list_.get(tableOfQuery).get
         var tuple_key = ""
         var tuple_value = ""
         for (r <- resultSet) {
           tuple_key = r(0)
           for(rr <- r){
             tuple_value += rr
             tuple_value += '^'
           }
           tuple_value += '~'
           table.insertOneTuple(tuple_key, tuple_value.getBytes)
           //printf(tuple_value+"\n")
           tuple_value = ""
         }
      }
      case "select" => {
        var table = tableManager.table_list_.get(tableOfQuery).get
        var tuple_key = resultSet(0)(0).toInt
        var indexPosition = table.position_list_(tuple_key)
        var index_list = new ArrayBuffer[IndexPosition]
        index_list += indexPosition
//         var tuple_value = ""
        //printf(tuple_key)
        tableManager.getRecordFromChunk(index_list)
         //Todo
            
      }
      case _ => print("Wrong Sql query!")
    }
  }
      
  
  /**
   * Simply parse the SQL and return the key information.
   */
  def processSQL(query: String): String = {
    //Todo
    val ssp = new SQLSimpleParser()
    ssp.parser(query)
    if(ssp.getParseStatus()){
      //Query type
      var queryType = ssp.getQueryType()
      println("Query type: "+queryType)
      //Table
      var queryTable = ssp.getTableName()
      println("Table: "+queryTable)
      //Result
      //println("Result:")
      val rs = ssp.getResult()
      for(x<-rs){
        for(xx<-x){
          print(xx)
        }
        println("")
      }
      println(" \n")
      if(tableManager.table_list_.contains(queryTable)){
        parserQuery(queryType,queryTable,rs)
      } else
        println("Table "+queryTable+" is not exist!\n");

    }
    "\"This sentence pretends to be a result.\""
  }
  
  /**
   * The listener getting command from the client.
   */
  class ServerListener(port: Int) extends Runnable{
    //Socket connection
    val serverSocket = new ServerSocket(port)
    //Start connection
    val socket = serverSocket.accept()
    //The InputStream receiving data from Client
    val inStream = new DataInputStream(socket.getInputStream())
    //The OutputStream sending data to Client
    val outStream = new DataOutputStream(socket.getOutputStream())
    
    /**
     * Running the thread to receiving messages from Client.
     */
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
    
    /**
     * Return message to the Client.
     */
    def returnInfo(info: String) = {
      outStream.writeUTF(info);
    }
  }
  
}

object Server{
  /**
   * The entrance of the Server.
   */
  def main(args: Array[String]){
    println("Starting...")
    val server = new Server()
    //initialize the resource, including loading data into the database
    server.initializeServer()
    //start the listener for SQL query
    server.startListener()
  }
}