import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ProcessorInterface extends Remote {
    void submeteScript(byte[] script, String idFicheiro, String idPedido) throws RemoteException;
    String getEstado(String idPedido) throws RemoteException;
    void submeteScriptReplica(byte[] script, String idFicheiro, String idPedido) throws RemoteException;
}
