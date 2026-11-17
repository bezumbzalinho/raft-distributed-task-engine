public class Requests {
    private final byte[] script;
    private final String idPedido;
    private final String idFicheiro;

    public Requests(byte[] _script, String _idPedido, String _idFicheiro) {
        script = _script;
        idPedido = _idPedido;
        idFicheiro = _idFicheiro;
    }
    public byte[] getScript() {
        return script;
    }

    public String getIdFicheiro() {
        return idFicheiro;
    }

    public String getIdPedido() {
        return idPedido;
    }
}
