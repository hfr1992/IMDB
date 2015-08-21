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
    private var chunk_id:ChunkID,
    private var tuple_offset:Long) {
  var chunk_id_ = chunk_id
  var tuple_offset_ = tuple_offset
}