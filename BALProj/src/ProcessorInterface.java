import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ProcessorInterface extends Remote {
    void submeteScript(byte[] script, String idFicheiro, String idPedido) throws RemoteException;
    String getEstado(String idPedido) throws RemoteException, InterruptedException;
    void submeteScriptReplica(byte[] script, String idFicheiro, String idPedido, String address) throws RemoteException;
    void resumeTasks(String deadProcessor, String ReplicationProcessor) throws RemoteException;
    void removeReplication(String idPedido) throws RemoteException;
    void putReplicaAddress(String address) throws RemoteException;
}
