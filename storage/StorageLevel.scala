/**
 * 07.Aug.2015
 * @author Suijun_524457
 */

package storage

import java.io.{ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap


/**
 * The [[StorageLevel$]] singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 */

class StorageLevel(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useHdfs: Boolean){

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  
  def useDisk = _useDisk
  def useMemory = _useMemory
  def useHdfs = _useHdfs
}
/**
 * Various [[storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  val NONE = new StorageLevel(false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false)
  val MEMORY_ONLY = new StorageLevel(false, true, false)
  val DESIRIABLE_STORAGE_LEVEL = new StorageLevel(false, true, false)
  val HDFS_ONLY = new StorageLevel(false, false, true)
  val MEMORY_AND_HDFS = new StorageLevel(false, true, true)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false)
}
