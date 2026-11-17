import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CoordinatorInterface extends Remote {
    void replication(byte[] script, String idFicheiro, String idPedido, String processor) throws RemoteException;
    String getReplication(String myAddress) throws RemoteException, InterruptedException;
    Integer getCountProcessors() throws RemoteException, InterruptedException;
}
