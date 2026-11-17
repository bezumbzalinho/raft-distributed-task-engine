import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface StorageInterface extends Remote {
    String uploadFileToServer(byte[] mybyte) throws IOException, InterruptedException;
    byte[] getFile(String idFicheiro) throws RemoteException;
    void submitOutput(byte[] mybyte, String idPedido) throws IOException;
    void receiveVotes(String type) throws NotBoundException, InterruptedException, IOException;
}
