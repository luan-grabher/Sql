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
import java.util.regex.Matcher;

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
        setConfigByFile(new File(configFilePath));
        setConnection();
        close();
    }

    public Database(File configFile) {
        setConfigByFile(configFile);
        setConnection();
        close();
    }

    /**
     * Tenta setar a conexão.
     *
     * @return se esta conectando no banco de dados sem erro.
     */
    public boolean testConnection() {
        try {
            setConnection(); //Define a conexão, vai gerar erro se não conseguir            
        } catch (Error e) {
            e.printStackTrace();
            return false;
        } finally {
            close();
        }
        return true;
    }

    /**
     * Fecha a conexão se estiver aberta e seta novamente
     */
    private void reConnect() {
        close();
        setConnection();
    }

    /**
     * Fecha conexão e se ocorrer algum erro, causa erro
     */
    public void close() {
        try {
            if (con != null) {
                DatabaseConnection.closeConnection(con);
                con.close();
            }
        } catch (SQLException e) {
            throw new Error(e);
        }
    }

    /**
     * Define a veriavel de conexão com as configurações atuais. Causa um erro
     * caso não conecte.
     */
    private void setConnection() {
        try {
            con = DatabaseConnection.getConnection(DRIVER, URL, USER, PASS);
            if (con.isClosed()) { //Se conexão estiver fechada
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

    /**
     * Define as variaveis de conexão com um arquivo de conexão. Se não tiver no
     * padrão causa Error
     */
    private void setConfigByFile(File configFile) {
        String[] cfg = FileManager.getText(configFile).split(";"); //Divide texto do arquivo

        if (cfg.length > 3) {
            DRIVER = cfg[0];
            URL = cfg[1];
            USER = cfg[2];
            PASS = cfg[3];
        } else {
            throw new Error("Arquivo de configuração não está no padrão!");
        }
    }

    /**
     * Executa vários códigos SQL. Continua executando se der erro em algum.
     *
     * @param batchs Códigos SQL
     * @return Retorna o número de códigos que foram executados com sucesso.
     */
    public int executeBatchs(String[] batchs) {
        int counts = 0;

        //Não colocar try catch para o código travar caso der erro
        for (String batch : batchs) {//Percorre tosdos codigos
            if (!"".equals(batch.replaceAll(" ", ""))) { //Se o codigo nao estiver em branco
                try {
                    query(batch);
                    counts++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        close();
        return counts;
    }

    /**
     * Executa o código do arquivo SQL e retorna true se não ocorrer erro no
     * código.
     *
     * @param sqlFile Arquivo SQL com código
     * @return retorna true se não ocorrer erro no código
     */
    public boolean query(File sqlFile) throws SQLException {
        return query(sqlFile, null);
    }

    /**
     * Executa o código do arquivo SQL ,substitui as variaveis do mapa e retorna
     * true se não ocorrer erro no código.
     *
     * @param sqlFile Arquivo SQL com código
     * @param variableChanges Variaveis para substiuir dentro do código
     * @return retorna true se não ocorrer erro no código
     */
    public boolean query(File sqlFile, Map<String, String> variableChanges) throws SQLException {
        String text = FileManager.getText(sqlFile);
        text = replaceVariableChanges(text, variableChanges);
        return query(text);
    }

    /**
     * Executa o código SQL ,substitui as variaveis do mapa e retorna true se
     * não ocorrer erro no código.
     *
     * @param sqlScript código SQL
     * @param variableChanges Variaveis para substiuir dentro do código
     * @return retorna true se não ocorrer erro no código
     */
    public boolean query(String sqlScript, Map<String, String> variableChanges) throws SQLException {
        sqlScript = replaceVariableChanges(sqlScript, variableChanges);
        return query(sqlScript);
    }

    /**
     * Executa o código SQL e retorna true se não ocorrer erro no código.
     *
     * @param sqlScript Código SQL para executar
     * @return retorna true se não ocorrer erro no código
     * @throws java.sql.SQLException Causa um erro caso algo ocorra errado
     */
    public boolean query(String sqlScript) throws SQLException {
        if (!sqlScript.equals("")) { //Se o código não estiver vazio
            reConnect();

            try {
                PreparedStatement stmt = con.prepareStatement(sqlScript);
                stmt.executeUpdate();//Executa codigo
                DatabaseConnection.closeConnection(con, stmt);//fecha stmt
                close();//fecha conexao
                return true;
            } catch (SQLException ex) {
                close();
                throw new SQLException("SQL: '" + sqlScript + "'\nErro: " + getStackTrace(ex));
            }
        } else {
            throw new SQLException("O código SQL não pode ficar em branco!");
        }
    }

    /**
     * Substitui todas as variaveis do mapa dentro do script
     */
    private String replaceVariableChanges(String sqlScript, Map<String, String> swaps) {
        if (swaps != null) {//Se o mapa nao for nulo
            String[] newStr = new String[1];//Tem que usar array para acessar variavel dentro do foreach
            newStr[0] = sqlScript;
            
            swaps.forEach((key,val) ->{
                newStr[0] = newStr[0].replaceAll(":" + key, Matcher.quoteReplacement(val));//matcher para remover barras e cifroes
            });
            
            sqlScript = newStr[0];
        }
        
        //Tira o que nao teve replace
        sqlScript = sqlScript.replaceAll("[:][a-zA-Z]+", "");

        return sqlScript;
    }

    /**
     * Retorna uma lista de string das colunas de uma pesuqisa sql do arquivo
     *
     * @param sqlFile Arquivo com Código sql select
     * @return Retorna uma lista de string das colunas de uma pesuqisa sql
     */
    public ArrayList<String[]> select(File sqlFile) {
        return select(FileManager.getText(sqlFile));
    }

    /**
     * Retorna uma lista de string das colunas de uma pesuqisa sql do arquivo e
     * substitui variaveis com o mapa de trocas
     *
     * @param sqlFile Arquivo com Código sql select
     * @param variableChanges Mapa de trocas
     * @return Retorna uma lista de string das colunas de uma pesuqisa sql
     */
    public ArrayList<String[]> select(File sqlFile, Map<String, String> variableChanges) {
        return select(FileManager.getText(sqlFile), variableChanges);
    }

    /**
     * Retorna uma lista de string das colunas de uma pesuqisa sql do arquivo e
     * substitui variaveis com o mapa de trocas
     *
     * @param sqlScript Código sql select
     * @param variableChanges Mapa de trocas
     * @return Retorna uma lista de string das colunas de uma pesuqisa sql
     */
    public ArrayList<String[]> select(String sqlScript, Map<String, String> variableChanges) {
        sqlScript = replaceVariableChanges(sqlScript, variableChanges);
        return select(sqlScript);
    }

    /**
     * Retorna uma lista de string das colunas de uma pesuqisa sql
     *
     * @param sql Código sql select
     * @return Retorna uma lista de string das colunas de uma pesuqisa sql
     */
    public ArrayList<String[]> select(String sql) {
        ArrayList<String[]> result = new ArrayList<>(); //Inicia resultado

        try {
            ResultSet rs = getResultSet(sql); // Pega o result set do script

            int columnCount = rs.getMetaData().getColumnCount();//conta colunas
            while (rs.next()) {//Enquanto tiver proximo pra ir, vai pro proximo
                String[] row = new String[columnCount];//Cria array da linha
                for (int i = 0; i < columnCount; i++) {//percorre colunas
                    row[i] = rs.getString(i + 1);//define a coluna
                }
                result.add(row);//adiciona linha na lista de linhas
            }
        } catch (SQLException e) {
            throw new Error(e);
        }

        close();
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
     * DEPOIS. Causa Erro se ocorrer algum erro
     *
     * @param sql Script SQL com Select
     * @return Retorna o record Set do comando ou causa Erro
     */
    public ResultSet getResultSet(String sql) {
        reConnect(); //reconecta no banco

        if (!sql.equals("") && con != null) {
            try {
                PreparedStatement stmt = con.prepareStatement(sql);
                return stmt.executeQuery();
            } catch (SQLException ex) {
                StringBuilder sb = new StringBuilder();
                sb.append("Erro no select com o script:").append(sql).append("\n");
                sb.append(getStackTrace(ex));
                throw new Error(sb.toString());
            }
        } else {
            throw new Error("O código SQL está em branco ou a conexão está inválida");
        }
    }

    /**
     * Converte o ResultSet para uma Lista de Mapas, cada Mapa represeta uma
     * linha com colunas
     *
     * @param rs ResultSet da pesquisa
     * @return Lista de Mapas, cada Mapa represeta uma linha com colunas
     * @throws SQLException
     */
    public List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData(); //Define metadata
        int columns = md.getColumnCount(); //Pega numero de colunas
        List<Map<String, Object>> rows = new ArrayList<>(); //Cria lista
        while (rs.next()) {//Vai para a proxima linha
            Map<String, Object> row = new HashMap<>(); //Cria mapa da linha
            for (int i = 1; i <= columns; ++i) {//percorre todas colunas
                row.put(md.getColumnLabel(i), rs.getObject(i)); //coloca coluna no mapa da linha
            }
            rows.add(row); //adiciona a linha na lista de linhas
        }
        return rows; //retorna as linhas
    }

    /**
     * Pega uma lista de mapas com as colunas nomeadas. CAUSA ERRO
     *
     * @param sql Script SQL com código Select
     * @param swaps mapa com trocas que devem ser feitas no SQL script. Caso não
     * tenha trocas, pode deixar null.
     * @return Retorna lista de mapas com as colunas nomeadas ou causa erro.
     */
    public List<Map<String, Object>> getMap(String sql, Map<String, String> swaps) {
        if (swaps != null) sql = replaceVariableChanges(sql, swaps);

        ResultSet rs = getResultSet(sql); //Pega o result set

        try {
            List<Map<String, Object>> list = resultSetToList(rs);
            close();
            return list;
        } catch (SQLException ex) {
            throw new Error(ex);
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
