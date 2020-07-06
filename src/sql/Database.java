package sql;

import fileManager.FileManager;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

public class Database {

    private static Database database;

    public static void setStaticObject(Database database) {
        Database.database = database;
    }

    public static Database getDatabase() {
        return database;
    }

    private Connection con = null;
    //public boolean conectado = false;

    private String DRIVER = "";
    private String URL = "";
    private String USER = "";
    private String PASS = "";

    public Database(String DRIVERC, String URLC, String USERC, String PASSC) {
        setConfigVariables(DRIVERC, URLC, USERC, PASSC);
        close();
    }

    public Database(String configFilePath) {
        getConfig(new File(configFilePath));
        setConfigVariables(DRIVER, URL, USER, PASS);
        close();
    }

    public Database(File configFile) {
        getConfig(configFile);
        setConfigVariables(DRIVER, URL, USER, PASS);
        close();
    }

    public boolean testConnection() {
        boolean b = false;
        try {
            b = !DatabaseConnection.getConnection(DRIVER, URL, USER, PASS).isClosed();
        } catch (Exception e) {
        }

        close();

        return b;
    }

    private void reConnect() {
        close();
        setConfigVariables(DRIVER, URL, USER, PASS);
    }

    public void close() {
        try {
            //query("");
            DatabaseConnection.closeConnection(con);
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Erro ao fechar conexao: " + e);
        }
    }

    private void setConfigVariables(String DRIVERC, String URLC, String USERC, String PASSC) {
        DRIVER = DRIVERC;
        URL = URLC;
        USER = USERC;
        PASS = PASSC;

        try {
            con = DatabaseConnection.getConnection(DRIVER, URL, USER, PASS);
            if (con == null) {
                System.out.println("Conexao inválida para:");
                System.out.println("DRIVER: " + DRIVER);
                System.out.println("URL: " + URL);
                System.out.println("USUÁRIO: " + USER);
                System.out.println("SENHA: " + PASS + "\n");
            }
        } catch (Exception e) {
            System.out.println("Ocorreu um erro ao conectar ao banco na classe BANCO: " + e);
            e.printStackTrace();
            System.out.println("Ab conexao usada foi:");
            System.out.println("DRIVER: " + DRIVER);
            System.out.println("URL: " + URL);
            System.out.println("USUÁRIO: " + USER);
            System.out.println("SENHA: " + PASS + "\n");
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
                if (batch.replaceAll(" ", "") != "") {
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
        reConnect();
        PreparedStatement stmt = null;
        ArrayList<String[]> result = new ArrayList<>();

        if (!sql.equals("")) {
            try {
                stmt = con.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    String[] row = new String[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = rs.getString(i + 1);
                    }
                    result.add(row);
                }
            } catch (SQLException ex) {
                System.out.println("Erro no select do banco: " + ex);
                System.out.println("O comando SQL usado foi: " + sql);
            } finally {
                DatabaseConnection.closeConnection(con, stmt);
                //conectado = false;
            }
        }
        close();
        return result;

    }
}
