package sql.Entity;

import Auxiliar.Valor;
import java.math.BigDecimal;
import java.util.List;

public class CampoSQL {

    private String nome;
    private String between;
    private Valor valor;
    private boolean circundarValorComAspas;
    private boolean habilitarNulo;

    public CampoSQL(String nome, String between, String valor) {
        Construtor(nome, between, valor, false, false);
    }

    public CampoSQL(String nome, String between, String valor, boolean habilitarNulo) {
        Construtor(nome, between, valor, false, habilitarNulo);
    }

    public CampoSQL(String nome, String between, String valor, boolean circundarValorComAspas, boolean habilitarNulo) {
        Construtor(nome, between, valor, circundarValorComAspas, habilitarNulo);
    }

    private void Construtor(String nome, String between, String valor, boolean circundarValorComAspas, boolean habilitarNulo) {
        this.nome = nome;
        this.between = between;
        this.valor = new Valor(valor);
        this.circundarValorComAspas = circundarValorComAspas;
        this.habilitarNulo = habilitarNulo;
    }

    public String getNome() {
        return nome;
    }

    public String getValor() {
        String retorno = valor.getString();
        if (habilitarNulo && valor.getBigDecimal().compareTo(BigDecimal.ZERO) == 0) {
            retorno = "null";
        }

        if (!retorno.equals("null") && circundarValorComAspas) {
            retorno = "'" + retorno + "'";
        }

        return retorno;
    }

    public String getBetween() {
        return between;
    }

    /*---------ESTACTICAS----------*/
    public static String imprimirCampos(List<CampoSQL> campos, String divisor) {
        try {
            StringBuilder sb = new StringBuilder();

            campos.forEach(campo -> {
                if (!sb.toString().equals("")) {
                    sb.append(divisor);
                }
                sb.append(campo.getNome());
                sb.append(campo.getBetween());
                sb.append(campo.getValor());
            });

            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

}
