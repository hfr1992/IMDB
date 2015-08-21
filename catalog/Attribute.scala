package catalog

/**
 * @author Suijun
 */
class Attribute(
    private var attr_name:String,
    private var attr_type:String,
    private var attr_length:Int){
  
  def this() = {
    this("","",0)
  }
  
  def setAttrName(attrName:String) = {
    attr_name_ = attrName
  }
  def getAttrName():String = {
    attr_name_
  }
  
  def setAttrType(attrType:String) = {
    attr_type_ = attrType
  }
  def getAttrType():String = {
    attr_type_
  }
  
  def setAttrLength(attrLength:Int) = {
    attr_length_ = attrLength
  }
  def getAttrLength():Int = {
    attr_length_
  }
  
  private var attr_name_ = attr_name
  private var attr_type_ = attr_type
  private var attr_length_ = attr_length
}