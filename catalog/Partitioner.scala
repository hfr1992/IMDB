package catalog

import common.ids._

/**
 * @author Suijun
 * 28, Aug, 2015
 */


/*
 * Basic information of Block
 * */
class BlockInfo (
    private var block_id:Long,
    private var tuple_count:Int){
  
  def this() {
    this(0L,0)
  }
  
  var block_id_ = block_id
  var tuple_count_ = tuple_count
}

/*
 * Basic information of partition.
 * Abstract class.
 * */
class PartitionInfo(
    private var partition_id: PartitionID,
    private var file_name:String,
    private var number_of_blocks:Long,
    private var number_of_tuples:Long) {
  
  /*
   * constructor of PartitionID
   * */
  def this(){
    this (new PartitionID(),"",0L,0L)
  }
  
  def this(partition_id: PartitionID, file_name:String){
    this(partition_id,file_name,0L,0L)
  }
  
  def this(partition_id: PartitionID){
    this(partition_id,"",0L,0L)
  }

  /*
   * Below are Abstract function
   * */
  def add_one_block() = {
    
  }
  
  def add_mutiple_block(number_of_new_blocks:Long) = {
    
  }
  def bind_one_block(blockid:Long, target:NodeID):Boolean = {
    false
  }
  def bind_all_blocks(target:NodeID):Boolean = {
    false
  }
  def unbind_all_blocks() = {
    
  }
  
  def is_all_blocks_bound() = {
    
  }
  def is_colocated():PartitionInfo = {
    new PartitionInfo()
  }
  def get_location():NodeID = {
    0
  }
  
  /*
   * Get the number of blocks in this Partition
   * */
  def get_number_of_blocks():Long = {
    number_of_blocks_
  }
  

  var hdfs_file_name_ : String = file_name
  var number_of_blocks_ :Long = number_of_blocks
  var partition_id_ :PartitionID = partition_id
  var number_of_tuples_ :Long = number_of_tuples
}

class Partitioner {
  
}
