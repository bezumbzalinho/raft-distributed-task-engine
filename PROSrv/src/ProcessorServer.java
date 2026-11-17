import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
public class ProcessorServer {
    public static void main(String[] args) {

        Registry r = null;
        try {
            r = LocateRegistry.createRegistry(Integer.parseInt(args[0])); // cria o registo para os clientes e o balanceador acederem aos processors
        } catch(RemoteException a) {
            a.printStackTrace();
        }

        try {
            Processor processor = new Processor("rmi://localhost:" + args[0] + "/processor"); // cria uma variável Processor que é a class que fica responsável por efetuar pedidos dos clientes

            r.rebind("processor", processor); // criar a referência remota, para os clientes acederem

            System.out.println("Processor " + args[0] + " is ready...");
        } catch(Exception e) {
            System.out.println("Storage: " + e.getMessage());
        }
    }
}
