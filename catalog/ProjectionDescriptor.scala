package catalog

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashSet
import common.ids._

/**
 * A projection is a combination of columns that belong to a single table and will be horizontally
 * partitioned among storage instances.
 * ProjectionDescriptor mainly contains two kinds of important information:
 *
 * 1). how many attributes there are in a projection and what are they.
 *
 * 2). how the projection is partitioned (e.g., hash partition, range partition). How many storage
 *    instances are involved.
 *    
 * For further use
 * @author Suijun
 * 28, Aug, 2015
 */


class ProjectionDescriptor(private var p_id:ProjectionID) {
  
  
  //  ProjectionOffset projection_offset_;
  var projection_id_ :ProjectionID = p_id
  var column_list_ :ArrayBuffer[Column] = null
  var partitioner: Partitioner = null

  /* The following is considered to be deleted, as the catalog module only has a logically view
   * of a table rather than detailed physical view such as filename, etc.
   */
  var fileLocations :HashMap[String,HashSet[String]] = null
  var hdfsFilePath : String = ""
  var blkMemoryLocations :HashMap[String,String] = null
  
  /* The following is deleted from version 1.2*/
  var Projection_name_ :String = ""  //projection does not need a string name.

  def addAttribute(attr:Attribute) = {
    //Todo
  }
  def hasAttribute(attr:Attribute):Boolean = {
    //Todo
    
    false
  }
  def DefinePartitonier(number_of_partitions:Long,partition_key:Attribute) = {
    //Todo
  }
  def getPartitioner() {
    //Todo
  }
  def isExist(name:String) :Boolean = {
    //Todo
    
    false
  }
  def setProjectionID(pid:ProjectionID) = {
    projection_id_ = pid
  }
  def getFileLocations() = {
    fileLocations
  }
  def getProjectionID() = {
    projection_id_
  }
  def AllPartitionBound():Boolean = {
    //Todo
    false
  }
  def getAttributeList() = {
    //Todo
  }
  
  def getAttributeIndex():Attribute = {
    //Todo
    
    new Attribute()
  }
  def getNumberOfTuplesOnPartition(partition_off:Long) = {
    //Todo
    
    0L
  }
  // return the sum of all attribute's length in projection, as this projection's cost
  def getProjectionCost() = {
    //Todo
    
    0L
  }
}