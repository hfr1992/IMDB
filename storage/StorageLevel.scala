package storage

/**
 * The StorageLevel singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 * @author Suijun
 * 26, Aug, 2015
 */

class StorageLevel(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useHdfs: Boolean){
  // Whether use disk to store data
  def useDisk = _useDisk
  // Whether use Memory to store data
  def useMemory = _useMemory
  // Whether use Hdfs to store data
  def useHdfs = _useHdfs
}

/*
 * Singleton object of StoragLevel
 * */
object StorageLevel {
  val NONE = new StorageLevel(false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false)
  val MEMORY_ONLY = new StorageLevel(false, true, false)
  val DESIRIABLE_STORAGE_LEVEL = new StorageLevel(false, true, false)
  val HDFS_ONLY = new StorageLevel(false, false, true)
  val MEMORY_AND_HDFS = new StorageLevel(false, true, true)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false)
}
