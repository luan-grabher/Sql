package sql;

import java.util.ArrayList;
import java.util.List;

public class SQL {
    private static int maxSplitIn = 1450;
    /**
     * @return comando SQL entre parenteses com função IN separada por no máximo
     * 1500 'ins'
     */
    public static String divideIn(String todosIns, String nomeCampo) {
        StringBuilder novoTexto = new StringBuilder();

        List<String> listaIn = splitWithJump(todosIns, ",", maxSplitIn);
        for (String in : listaIn) {
            if (!in.equals("")) {
                novoTexto.append(novoTexto.toString().equals("") ? "(" : " OR ");
                novoTexto.append("(");
                novoTexto.append(nomeCampo);
                novoTexto.append(" IN (");
                novoTexto.append(in);
                novoTexto.append(") )");
            }
        }

        novoTexto.append(")");

        return novoTexto.toString();
    }
    
    /**
     *  @return Lista Java de comandos utilizando a expressão IN
     */
    public static List<String> getListInWithMaxSplit(String todosIns, String nomeCampo) {
        List<String> listaIn = splitWithJump(todosIns, ",", 1450);
        for (int i = 0; i < listaIn.size(); i++) {
            listaIn.set(i, "( " + nomeCampo + " IN (" + listaIn.get(i) + "))");
            
        }

        return listaIn;
    }

    /**
     * @return Faz split a cada quantos pulos foi informado
     */
    public static List<String> splitWithJump(String stringOriginal, String delimiter, int jumps) {
        //inicializa retorno
        List<String> textosSeparadosPeloPuloMaximo = new ArrayList<>();
        //define número de pulo
        int pular = jumps - 1;

        String strAtual = stringOriginal;

        //Apagar em branco
        while (strAtual.contains(delimiter + delimiter)) {
            strAtual = strAtual.replaceAll(delimiter + delimiter, delimiter);
        }
        strAtual = strAtual.startsWith(delimiter) ? strAtual.substring(delimiter.length(), strAtual.length()) : strAtual;
        strAtual = strAtual.endsWith(delimiter) ? strAtual.substring(0, strAtual.length() - delimiter.length()) : strAtual;

        
        //percorrer
        while (!strAtual.equals("")) {
            //Separa a string pelo delimitador até os últimos "jumps" (Ex: 1500) --> tamanho do split - o pulo
            int maxSplit = strAtual.split(delimiter, -1).length > pular ? strAtual.split(delimiter, -1).length - pular : 0;
            String[] separadosComFiltroNoUltimo = maxSplit!=0?strAtual.split(delimiter, maxSplit):new String[]{strAtual};

            //Adiciona ao texto final os ultimos que ficam limitados ao limite do pulo
            textosSeparadosPeloPuloMaximo.add(separadosComFiltroNoUltimo[separadosComFiltroNoUltimo.length - 1]);

            //Junta novamente os arrays que nao foram separados
            strAtual = join(separadosComFiltroNoUltimo, delimiter, separadosComFiltroNoUltimo.length - 1);
        }
        return textosSeparadosPeloPuloMaximo;
    }

    public static String join(String[] strArray, String joinString) {
        return join(strArray, joinString, 0, strArray.length);
    }

    public static String join(String[] strArray, String joinString, long max) {
        return join(strArray, joinString, 0, max);
    }

    public static String join(String[] strArray, String joinString, long min, long max) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < max; i++) {
            String string = strArray[i];
            sb.append(sb.toString().equals("") ? "" : joinString);
            sb.append(string);
        }
        return sb.toString();
    }
}
