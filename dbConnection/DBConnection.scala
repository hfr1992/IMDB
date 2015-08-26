package dbConnection

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

/**
 * @author Feiran
 * Using JDBC to connect to the PostgreSQL
 */
class DBConnection {
    /*
     * Port: 5432
     * Database: test
     * Username: postgres
     * Password: 123456
     */
    val conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "123456")
    
    /**
     * Select query.
     */
    def query(query_statement: String) = {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      statement.executeQuery(query_statement)
    }
    
    /**
     * Insert query.
     */
    def insert(insert_statement: String) = {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val prep = conn.prepareStatement(insert_statement)
      prep.executeUpdate
    }
}