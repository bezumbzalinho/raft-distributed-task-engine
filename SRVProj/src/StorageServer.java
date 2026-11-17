import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class StorageServer {
    public static void main(String[] args) {
        Registry r = null;

        try {
            r = LocateRegistry.createRegistry(Integer.parseInt(args[0])); // cria um registo para os clientes acederem, que vai correr no porto 2022
        } catch(RemoteException a) {
            a.printStackTrace();
        }

        try {
            Storage storage =  new Storage("rmi://localhost:" + args[0] + "/storage"); // cria uma variável Storage que é a class que fica responsável por guardar na pasta o conteúdo que recebe

            r.rebind("storage", storage ); // criar uma referência remota, para os clientes acederem

            System.out.println("Storage " + args[0] + " is ready...");
        } catch(Exception e) {
            System.out.println("Storage: " + e.getMessage());
        }
    }
}
