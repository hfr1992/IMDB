package catalog

import common.ids._

/**
 * Column is use to store the information of a column data, extends from Attribute.
 * For further use...
 * @author Suijun
 * 28, Aug, 2015
 */
class Column(
    private var attr_name:String,
    private var attr_type:String,
    private var attr_length:Int,
    private var column_id:ColumnID) extends Attribute(attr_name,attr_type,attr_length){

  // id of a column
  var column_id_ = column_id
  
  
  def this() {
    this("","",0,new ColumnID())
  }
  
  def this (c:Column) {
    this(c.getAttrName(), c.getAttrType(), c.getAttrLength(), c.column_id_)
  }
}