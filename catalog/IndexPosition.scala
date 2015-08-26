package catalog

/**
 * IndexPosition is the data structure of the position where the record store in, it contains a chunk id and a offset.
 * IndexPositionMap is the key-value pair of index key and record position. One key map to a IndexPosition.
 * @author Suijun
 * 26, Aug, 2015
 * */

/*
 * Structure of record position.
 * Use new IndexPosition(String,Long) to construct a IndexPosition.
 * */
class IndexPosition(
    private var chunk_id:String,
    private var tuple_offset:Long) {
  
  // Chunk id of the position.
  var chunk_id_ = chunk_id
  // Record offset of the position.
  var tuple_offset_ = tuple_offset
}

/*
 * Structure of index key and record position key-value pair
 * Use new IndexPositionMap(String,IndexPosition) to construct a IndexPosition.
 * */
class IndexPositionMap(
    private var key:String,
    private var index_position:IndexPosition){
  
  // Index key of the IndexPositionMap.
  var key_ = key
  // Record position of the IndexPositionMap.
  var index_position_ = index_position
}