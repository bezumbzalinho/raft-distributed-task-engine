import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Main {
    public static void main(String[] args) {

        Registry r = null;
        try {
            r = LocateRegistry.createRegistry(2023); // cria um registo para os clientes acederem, que vai correr no porto 2023
        } catch(RemoteException a) {
            a.printStackTrace();
        }
        try {
            Balancer balancer =  new Balancer(); // cria uma variável Balancer que é a class que fica responsável por escolher qual processor deve executar o pedido do cliente

            r.rebind("balancer", balancer ); // criar uma referência remota, para os clientes acederem

            System.out.println("Balancer is ready...");
        } catch(Exception e) {
            System.out.println("Storage: " + e.getMessage());
        }
    }
}