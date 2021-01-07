package sql;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseConnection {

    public static Connection getConnection(String DRIVER, String URL, String USER, String PASS) throws SQLException {
        try {
            try {
                Class.forName(DRIVER);
            } catch (ClassNotFoundException e) {
                throw new SQLException("Classe do driver SQL não encontrada!");
            }

            Connection conn = DriverManager.getConnection(URL, USER, PASS);
            return conn;
        } catch (SQLException ex) {
            StringBuilder sb = new StringBuilder("Conexao inválida para:\n");
            sb.append("DRIVER: ").append(DRIVER).append("\n");
            sb.append("URL: ").append(URL).append("\n");
            sb.append("USUÁRIO: ").append(USER).append("\n");
            sb.append("SENHA: ").append(PASS).append("\n\n");
            sb.append("SENHA: ").append(PASS).append("\n\n");
            sb.append("Erro Java: ").append(getStackTrace(ex));
            
            throw new SQLException(sb.toString());
        }
    }

    public static void closeConnection(Connection con) throws SQLException {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (SQLException ex) {
                throw new SQLException("Erro ao fechar conexão : " + getStackTrace(ex));
            }
        }
    }

    public static void closeConnection(Connection con, PreparedStatement stmt) throws SQLException {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {
                throw new SQLException("Erro ao fechar conexão com stm: " + getStackTrace(ex));
            }
        }
        closeConnection(con);
    }

    public static void closeConnection(Connection con, PreparedStatement stmt, ResultSet rs) throws SQLException {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                throw new SQLException("Erro ao fechar conexão com stm e rs: " + getStackTrace(ex));
            }
        }
        closeConnection(con, stmt);
    }

    public static String getStackTrace(SQLException e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        return sw.toString();
    }
}
