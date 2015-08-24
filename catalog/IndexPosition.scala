package catalog

/**
 * @author ͯSuijun
 * 
 * Data structure of ChunkID,HdfsBlock,ChunkInfo,IndexPosition
 * */
import storage.StorageLevel

/*
 * Structure of index position
 * */
class IndexPosition(
    private var chunk_id:String,
    private var tuple_offset:Long) {
  var chunk_id_ = chunk_id
  var tuple_offset_ = tuple_offset
}

class IndexPositionMap(
    private var key:String,
    private var index_position:IndexPosition){
  var key_ = key
  var index_position_ = index_position
}