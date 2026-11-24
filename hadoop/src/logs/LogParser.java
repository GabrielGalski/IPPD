package logs;

/**
 * Faz o parsing de uma linha CSV de log: timestamp,ip,recurso,status,bytes.
 * Retorna null para o cabeçalho e para linhas malformadas (que não são contadas).
 */
public final class LogParser {
    public static final int TIMESTAMP = 0, IP = 1, RECURSO = 2, STATUS = 3, BYTES = 4;

    private LogParser() {}

    public static String[] parse(String linha) {
        if (linha == null || linha.isEmpty() || linha.startsWith("timestamp,")) {
            return null; // cabeçalho (pode aparecer apenas no split que começa no byte 0)
        }
        String[] campos = linha.split(",", -1);
        return campos.length == 5 ? campos : null;
    }
}
