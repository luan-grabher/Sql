package testes;

import java.util.ArrayList;
import sql.Database;
import sql.SQL;

public class Teste {

    public static void main(String[] args) {
        testeSplitIn();
        System.exit(0);
    }

    private static void testeSplitIn() {
        System.out.println(SQL.divideIn("1,2,3,4,5,6,7", "CAMPOBANCO"));
    }

    private static void testeBanco() {
        String localArquivo = "./mysql.cfg";

        /*hehehehe
        add
        ad
        ad*/
        Database db = new Database(localArquivo);

        ArrayList<String[]> membros;
        membros = db.select("Select * from membros");

        for (int i = 0; i < membros.size(); i++) {
            System.out.println("Nome: " + membros.get(i)[1]);
        }

        System.out.println("tudo ok");
    }

}
