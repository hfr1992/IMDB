package socket

import java.net.ServerSocket
import java.net.Socket
import java.io._

/**
 * @author Feiran
 * The Client is the class sending the command to the Server class. It create a thread to listen user's input.
 */
class Client(url: String, port: Int) extends Runnable {
  //Socket connection
  val socket = new Socket(url, port)
  //The OutputStream sending data to Server
  val out = new DataOutputStream(socket.getOutputStream())
  //The InputStream receiving data from Client
  val in = new DataInputStream(socket.getInputStream())
  
  /**
   * Running the thread to sending messages to Server.
   */
  def run(){
    while(true){
      println(in.readUTF())
      var ln = io.Source.stdin.getLines()
      val query = ln.next()
      //If the user input is "quit", terminate the thread
      if(query=="quit"){
        println("Good bye.")
        return
      }else{
        out.writeUTF(query)
      }
    }
    
  }
}

object Client{
  /**
   * The entrance of the Client.
   */
  def main(args: Array[String]){
    println("Connecting to Server...")
    //The communication port is 8765
    (new Thread(new Client("localhost", 8765))).start()
  }
}