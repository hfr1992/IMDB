package dbConnection

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

/**
 * @author hfr
 */
class DBConnection {
    val conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "123456")
    
    def query(query_statement: String) = {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      statement.executeQuery(query_statement)
    }
    
    def insert(insert_statement: String) = {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val prep = conn.prepareStatement(insert_statement)
      prep.executeUpdate
    }
}