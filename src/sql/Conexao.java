
package sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class Conexao {
    public static Connection getConnection(String DRIVER,String URL, String USER, String PASS){
        try {
            try{
                Class.forName(DRIVER);
            }
            catch (ClassNotFoundException e)
            {
                System.err.println("Cannot load driver...");
                e.printStackTrace();
                return null;
            }
            
            Connection conn = DriverManager.getConnection(URL, USER, PASS);
            return conn;
        } catch (SQLException ex) {
            System.out.println("ERRO NA CLASSE (CONEXAO): " + ex);
            System.out.println("Conexao inválida para:");
            System.out.println("DRIVER: " + DRIVER);
            System.out.println("URL: " + URL);
            System.out.println("USUÁRIO: " + USER);
            System.out.println("SENHA: " + PASS + "\n");
            return null;
        }
    }
    
    
    public static void closeConnection(Connection con){
       if (con != null){
           try {
               con.close();
               con = null;
           } catch (SQLException ex) {
               System.out.println("Erro: " + ex);
           }
       } 
    }
    public static void closeConnection(Connection con, PreparedStatement stmt){
       if (stmt != null){
           try {
               stmt.close();
           } catch (SQLException ex) {
               System.out.println("Erro: " + ex);
           }
       }
       
        closeConnection(con);
    }
    public static void closeConnection(Connection con, PreparedStatement stmt, ResultSet rs){
        if (rs != null){
            try {
               rs.close();
            } catch (SQLException ex) {
               System.out.println("Erro: " + ex);
            }
        }
        closeConnection(con,stmt);
    }
}
