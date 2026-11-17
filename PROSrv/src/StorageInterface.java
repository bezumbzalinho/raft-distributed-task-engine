import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface StorageInterface extends Remote {
    String uploadFileToServer(byte[] mybyte) throws IOException, InterruptedException;
    byte[] getFile(String idFicheiro) throws RemoteException;
    void submitOutput(byte[] mybyte, String idPedido) throws IOException;
}
