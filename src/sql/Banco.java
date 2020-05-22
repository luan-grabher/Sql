package sql;

import fileManager.FileManager;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

public class Banco {

    private Connection con = null;
    //public boolean conectado = false;

    private String DRIVER = "";
    private String URL = "";
    private String USER = "";
    private String PASS = "";

    public Banco(String DRIVERC, String URLC, String USERC, String PASSC) {
        setConfigVariables(DRIVERC, URLC, USERC, PASSC);
        close();
    }
    
    public Banco(String configFilePath){
        getConfig(new File(configFilePath));
        setConfigVariables(DRIVER, URL, USER, PASS);
        close();
    }

    public Banco(File configFile) {        
        getConfig(configFile);
        setConfigVariables(DRIVER, URL, USER, PASS);
        close();
    }

    public boolean testConnection() {
        boolean b = false;
        try {
            b = !Conexao.getConnection(DRIVER, URL, USER, PASS).isClosed();
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
            Conexao.closeConnection(con);
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
            con = Conexao.getConnection(DRIVER, URL, USER, PASS);
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

    public boolean query(File sqlFile){
        return query(sqlFile, null);
    }

    public boolean query(File sqlFile, Map<String,String> variableChanges){
        String text = FileManager.getText(sqlFile);
        
        if(variableChanges != null){
            for (Map.Entry<String, String> variableChange : variableChanges.entrySet()) {
                String variable = variableChange.getKey();
                String value = variableChange.getValue();

                text = text.replaceAll(":" + variable, value);
            }
        }
        
        return query(text);
    }

    public boolean query(String sql) {
        boolean b = false;
        if (!sql.equals("")) {
            reConnect();
            PreparedStatement stmt = null;

            try {
                stmt = con.prepareStatement(sql);
                stmt.executeUpdate();
                b = true;
            } catch (SQLException ex) {
                if (!sql.equals("")) {
                    System.out.println("SQL: '" + sql + "'\nErro: " + ex);
                }
            } catch (StackOverflowError e) {

            } finally {
                Conexao.closeConnection(con, stmt);
            }
            close();
        }
        
        return b;
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
                Conexao.closeConnection(con, stmt);
                //conectado = false;
            }
        }
        close();
        return result;

    }
}
