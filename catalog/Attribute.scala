package catalog

/**
 * Structure of Attribute, there are three way to construct a Attribute: new Attribute(), 
 *                                                                       new Attribute(Attribute),
 *                                                                       new Attribute(String,String,[Int])
 * Attribute contain the detail of a column. According to variable we can know which table the column belong, 
 * whether the attribute is unique
 * @author Suijun
 * 26, Aug, 2015
 */

class Attribute(
    private var attr_name:String,
    private var attr_type:String,
    private var attr_length:Int){
  
  /*
   * Empty Constructor of Attribute
   * In this situation, the attr_namen and attr_type will be initialized as "", attr_length will be initialized as 0.
   * Don't Suggest to using this way to construct a new Attribute.
   * */
  def this() = {
    this("","",0)
  }
  
  /*
   * Construct Attribute from another Attribute
   * This function will copy all the variable from the given Attribute.
   * */
  def this(att:Attribute) = {
    this(att.getAttrName(), att.getAttrType(), att.getAttrLength())
    table_id_ = att.getTableId()
    index_ = att.getIndex()
    unique_ = att.getUnique()
  }
  
  
  /* Set/Get function of attr_name_ */
  def setAttrName(attrName:String) = {
    attr_name_ = attrName
  }
  def getAttrName():String = {
    attr_name_
  }
  
  /* Set/Get function of attr_type_ */
  def setAttrType(attrType:String) = {
    attr_type_ = attrType
  }
  def getAttrType():String = {
    attr_type_
  }
  
   /* Set/Get function of attr_length_ */
  def setAttrLength(attrLength:Int) = {
    attr_length_ = attrLength
  }
  def getAttrLength():Int = {
    attr_length_
  }
  
   /* Set/Get function of table_id_ */
  def setTableId_(tableId:String) = {
    table_id_ = tableId
  }
  def getTableId():String = {
    table_id_
  }
  
   /* Set/Get function of index_ */
  def setIndex_(index:Int) = {
    index_ = index
  }
  def getIndex():Int = {
    index_
  }
  
   /* Set/Get function of unique_ */
  def setUnique_(unique:Boolean) = {
    unique_ = unique
  }
  def getUnique():Boolean = {
    unique_
  }

  /*
   * Attribute a < Attribute b means a is ahead of b.
   * It's true only when the table_id_ of a and b are same, and the index_ of a is less-than the index_ of b. 
   * */
  def < (att:Attribute) = {
    if (table_id_ == att.getTableId())
      index_ < att.getIndex()
    else
      false
  }
  
  /*
   * Attribute a == Attribute b means a and b are same Attribute.
   * It's true only when both the table_id_ and index_ of a and b are same.
   * */
  def == (att:Attribute) = {
    table_id_ == att.getTableId()&&index_ == att.getIndex()
  }
  
  /*
   * Attribute a != Attribute b means a and b are not the same Attribute.
   * */
  def != (att:Attribute) = {
    !(table_id_ == att.getTableId()&&index_ == att.getIndex())
  }

  /* Whether the Attribute is unique */
  def isUnique() = {
    unique_
  }
  
  /*
   * Below are the variable of Attribute.
   * Note that all the variable is private, you can only access those variable by set/get function
   * */
  // Name of the Attribute.
  private var attr_name_ = attr_name
  // Type of the Attribute.
  private var attr_type_ = attr_type
  // Length of the Attribute.
  private var attr_length_ = attr_length
  // Id of the table which this Attribute belong.
  private var table_id_ = ""
  // The index of this Attribute in the table.
  private var index_ = 0
  // Whether the Attribute is unique.
  private var unique_ = false
}