package socket

import java.net.ServerSocket
import java.net.Socket
import java.io._

/**
 * @author hfr
 */
class Server{
  
  def initializeServer() = {
    //Hint
    println("Initializing...")
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