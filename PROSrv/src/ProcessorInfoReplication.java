// Informação relativa aos pedidos do processador que está a replicar
public class ProcessorInfoReplication {
    private final byte[] script;
    private final String address; // endereço do processador que está a replicar
    private final String idPedido;
    private final String idFicheiro;

    public ProcessorInfoReplication(byte[] _script, String _address, String _idPedido, String _idFicheiro) {
        script = _script;
        address = _address;
        idPedido = _idPedido;
        idFicheiro = _idFicheiro;
    }

    public byte[] getScript() {
        return script;
    }
    public String getAddress() {
        return address;
    }
    public String getIdFicheiro() {
        return idFicheiro;
    }
    public String getIdPedido() {
        return idPedido;
    }
}
