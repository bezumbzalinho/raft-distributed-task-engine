import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.LinkedList;

public interface ProcessorInterface extends Remote {
    void submeteScript(byte[] script, String idFicheiro, String idPedido) throws RemoteException;
    String getEstado(String idPedido) throws RemoteException, InterruptedException;
    void submeteScriptReplica(byte[] script, String idFicheiro, String idPedido, String address) throws RemoteException;
    void resumeTasks(String deadProcessor, String ReplicationProcessor) throws RemoteException;
    void removeReplication(String idPedido) throws RemoteException;
    void putReplicaAddress(String address) throws RemoteException;
    LinkedList<Requests> returnAllRequestsFromDeadProcessor(String address) throws RemoteException, InterruptedException;
    void receiveVotes(Integer inc) throws IOException, InterruptedException;
}
