package sql;

import fileManager.FileManager;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {

    //STATIC
    private static Database database;

    public static void setStaticObject(Database database) {
        Database.database = database;
    }

    public static Database getDatabase() {
        return database;
    }

    //MODEL
    private Connection con = null;

    private String DRIVER = "";
    private String URL = "";
    private String USER = "";
    private String PASS = "";

    public Database(String DRIVERC, String URLC, String USERC, String PASSC) {
        DRIVER = DRIVERC;
        URL = URLC;
        USER = USERC;
        PASS = PASSC;
        setConnection();
        close();
    }

    public Database(String configFilePath) {
        getConfig(new File(configFilePath));
        setConnection();
        close();
    }

    public Database(File configFile) {
        getConfig(configFile);
        setConnection();
        close();
    }

    /**
     * Tenta setar a conexão. 
     * @return se esta conectando no banco de dados sem erro.
     */
    public boolean testConnection() {
        try {
            setConnection(); //Define a conexão, vai gerar erro se não conseguir            
        } catch (Error e) {
            e.printStackTrace();
            return false;
        }finally{
            close();
        }
        return true;
    }

    /**
     * Fecha a conexão se estiver aberta e seta novamente
     */
    private void reConnect() {
        try {
            close();
            setConnection();
            if (con == null || con.isClosed()) {
                throw new Exception("Erro ao tentar");
            }
        } catch (Exception e) {
            
        }
    }

    public void close() {
        try {
            DatabaseConnection.closeConnection(con);
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Erro ao fechar conexao: " + e);
        }
    }

    /**
     * Define a veriavel de conexão com as configurações atuais. Causa um erro
     * caso não conecte.
     */
    private void setConnection() {
        try {
            con = DatabaseConnection.getConnection(DRIVER, URL, USER, PASS);
            if(con.isClosed()){ //Se conexão estiver fechada
                throw new SQLException("A conexão está fechada!");
            }
        } catch (SQLException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Erro ao conectar no banco de dados:\n");
            sb.append(getConfigDescription()).append("\n\n");
            sb.append(getStackTrace(e));

            throw new Error(sb.toString());
        }
    }

    private String[] getConfig(File configFile) {
        String txt = FileManager.getText(configFile);
        String[] cfg = txt.split(";");

        try {
            DRIVER = cfg[0];
            URL = cfg[1];
            USER = cfg[2];
            PASS = cfg[3];
        } catch (ArrayIndexOutOfBoundsException ex) {
        }

        return cfg;
    }

    public int executeBatchs(String[] batchs) {
        int counts = 0;

        try {
            for (String batch : batchs) {
                if (!"".equals(batch.replaceAll(" ", ""))) {
                    if (query(batch)) {
                        counts++;
                    }
                }
            }
        } catch (Exception e) {
        } finally {
            close();
            return counts;
        }
    }

    public boolean query(File sqlFile) {
        return query(sqlFile, null);
    }

    public boolean query(File sqlFile, Map<String, String> variableChanges) {
        String text = FileManager.getText(sqlFile);
        text = replaceVariableChanges(text, variableChanges);
        return query(text);
    }

    public boolean query(String sqlScript, Map<String, String> variableChanges) {
        sqlScript = replaceVariableChanges(sqlScript, variableChanges);
        return query(sqlScript);
    }

    public boolean query(String sqlScript) {
        boolean b = false;
        if (!sqlScript.equals("")) {
            reConnect();
            PreparedStatement stmt = null;

            try {
                stmt = con.prepareStatement(sqlScript);
                stmt.executeUpdate();
                b = true;
            } catch (SQLException ex) {
                if (!sqlScript.equals("")) {
                    System.out.println("SQL: '" + sqlScript + "'\nErro: " + ex);
                }
            } catch (StackOverflowError e) {

            } finally {
                DatabaseConnection.closeConnection(con, stmt);
            }
            close();
        }

        return b;
    }

    private String replaceVariableChanges(String sqlScript, Map<String, String> variableChanges) {
        if (variableChanges != null) {
            for (Map.Entry<String, String> variableChange : variableChanges.entrySet()) {
                String variable = variableChange.getKey();
                String value = variableChange.getValue();

                sqlScript = sqlScript.replaceAll(":" + variable, value);
            }
        }

        return sqlScript;
    }

    public ArrayList<String[]> select(File sqlFile) {
        return select(FileManager.getText(sqlFile));
    }

    public ArrayList<String[]> select(File sqlFile, Map<String, String> variableChanges) {
        return select(FileManager.getText(sqlFile), variableChanges);
    }

    public ArrayList<String[]> select(String sqlScript, Map<String, String> variableChanges) {
        sqlScript = replaceVariableChanges(sqlScript, variableChanges);
        return select(sqlScript);
    }

    public ArrayList<String[]> select(String sql) {
        ArrayList<String[]> result = new ArrayList<>();

        try {
            ResultSet rs = getResultSet(sql);

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getString(i + 1);
                }
                result.add(row);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ERRO[Database.select]: " + e.getMessage());
        } finally {
            close();
        }

        return result;
    }

    /**
     * Retorna o record set de uma pesquisa SQL (select). TEM QUE FECHAR CONEXÃO
     * DEPOIS
     *
     * @param sql Script SQL com Select
     * @param swaps Trocas de variáveis no script sql definidas por ":" na
     * frente da troca no script
     * @return Retorna o record Set do comando ou NULL em erro ou em branco
     */
    public ResultSet getResultSet(String sql, Map<String, String> swaps) {
        sql = replaceVariableChanges(sql, swaps);

        return getResultSet(sql);
    }

    /**
     * Retorna o record set de uma pesquisa SQL (select). TEM QUE FECHAR CONEXÃO
     * DEPOIS
     *
     * @param sql Script SQL com Select
     * @return Retorna o record Set do comando ou NULL em erro ou em branco
     */
    public ResultSet getResultSet(String sql) {
        ResultSet rs = null;

        reConnect();

        if (!sql.equals("") && con != null) {
            try {
                PreparedStatement stmt = con.prepareStatement(sql);
                rs = stmt.executeQuery();
            } catch (SQLException ex) {
                System.out.println("Erro no select do banco: " + ex);
                System.out.println("O comando SQL usado foi: " + sql);
                return null;
            }
        }
        //close();
        return rs;
    }

    /**
     * Convert the ResultSet to a List of Maps, where each Map represents a row
     * with columnNames and columValues
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columns; ++i) {
                row.put(md.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Pega uma lista de mapas com as colunas nomeadas
     *
     * @param sql Script SQL com código Select
     * @param swaps mapa com trocas que devem ser feitas no SQL script. Caso não
     * tenha trocas, pode deixar null.
     * @return Retorna lista de mapas com as colunas nomeadas
     */
    public List<Map<String, Object>> getMap(String sql, Map<String, String> swaps) {
        if (swaps != null) {
            sql = replaceVariableChanges(sql, swaps);
        }

        ResultSet rs = getResultSet(sql);

        try {
            List<Map<String, Object>> list = resultSetToList(rs);
            close();
            return list;
        } catch (SQLException ex) {
            ex.printStackTrace();
            close();
            return null;
        }
    }

    /**
     * Retorna o stacktrace em forma de string
     *
     * @param e Erro SQL
     * @return Retorna o stacktrace em forma de string
     */
    public static String getStackTrace(SQLException e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        return sw.toString();
    }

    /**
     * Retorna a configuração utilizada atual em STRING
     *
     * @return Retorna a configuração utilizada atual em STRING
     */
    public String getConfigDescription() {
        StringBuilder sb = new StringBuilder("DRIVER: ");
        sb.append(DRIVER).append("\n");
        sb.append("URL: ").append(URL).append("\n");
        sb.append("USUÁRIO: ").append(USER).append("\n");
        sb.append("SENHA: ").append(PASS);

        return sb.toString();
    }
}
