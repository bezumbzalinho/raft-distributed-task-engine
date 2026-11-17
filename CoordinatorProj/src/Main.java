import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Main {
    public static void main(String[] args) {

        Registry r = null;
        try {
            r = LocateRegistry.createRegistry(2022); // cria um registo que vai correr no porto 2022
        } catch(RemoteException a) {
            a.printStackTrace();
        }
        try {
            Coordinator coordinator = new Coordinator(); // cria uma variável Coordinator

            r.rebind("coordinator", coordinator ); // criar uma referência remota, para os clientes acederem

            System.out.println("Coordinator is ready...");
        } catch(Exception e) {
            System.out.println("Storage: " + e.getMessage());
        }
    }
}