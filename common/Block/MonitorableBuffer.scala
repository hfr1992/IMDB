package common.Block

/**
 * @author hfr
 */
class MonitorableBuffer() {
  var input_complete_ = true
  
  def setInputComplete() = {
    input_complete_ = true
  }
  
  def inputComplete() : Boolean = {
    input_complete_ == true
  }
}