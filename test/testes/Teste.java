package testes;

import java.io.File;
import java.util.ArrayList;
import sql.Database;

public class Teste {

    public static void main(String[] args) {
        Database.setStaticObject(new Database(new File("sci.cfg")));
        String sql = "select * from VSUC_EMPRESAS_TLAN l where BDCODEMP = 1 AND BDREFERENCIA = '202008'";
        
        ArrayList<String[]> a = Database.getDatabase().select(sql);
        System.exit(0);
    }

}
