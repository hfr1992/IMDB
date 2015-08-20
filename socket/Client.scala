package socket

import java.net.ServerSocket
import java.net.Socket
import java.io._

/**
 * @author hfr
 */
class Client(url: String, port: Int) extends Runnable {
  val socket = new Socket(url, port)
  val out = new DataOutputStream(socket.getOutputStream())
  val in = new DataInputStream(socket.getInputStream())
  
  def run(){
    while(true){
      println(in.readUTF())
      var ln = io.Source.stdin.getLines()
      val query = ln.next()
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
  def main(args: Array[String]){
    println("Connecting to Server...")
    (new Thread(new Client("localhost", 8765))).start()
  }
}