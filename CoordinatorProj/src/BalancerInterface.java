import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface BalancerInterface extends Remote {
    String[] submetePedido(byte[] script, String idFicheiro) throws RemoteException, InterruptedException, NotBoundException, MalformedURLException;
    void addProcessor(String processorAddress) throws RemoteException;
    void removeProcessor(String processorAddress) throws RemoteException;
    String getProcessorMoreFree() throws RemoteException, InterruptedException;
}
