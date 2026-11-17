import java.io.File;
import java.io.FileInputStream;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class Main {
    public static void main(String[] args) {

        StorageInterface storageInterface = null;

        BalancerInterface balancerInterface = null;

        String resposta = "";

        try {
            balancerInterface = (BalancerInterface) Naming.lookup("rmi://localhost:2023/balancer"); // conectar ao rmi do balancer

            storageInterface  = (StorageInterface) Naming.lookup(balancerInterface.getStorageLeader()); // conectar ao rmi do server

            File clientpathfile = new File("ola.txt"); //abrir um ficheiro qualquer com um conteúdo

            byte [] mydata=new byte[(int) clientpathfile.length()]; // criar um array de bytes para receber o conteúdo do ficheiro

            FileInputStream in=new FileInputStream(clientpathfile); // abrir o ficheiro em modo de leitura

            System.out.println("Uploading to storage...");

            in.read(mydata, 0, mydata.length); //converter o conteúdo do ficheiro para um array de bytes. Também podiamos converter para base64, entre outros...

            resposta = storageInterface.uploadFileToServer(mydata); // chamar a interface que está implementada na storage, que grava o conteúdo do ficheiro (dentro duma pasta no lado do servidor) e devolve o nome (único) criado desse novo ficheiro

            System.out.println("ID that the file has on the server side: " + resposta);

            in.close(); // fecha o modo de leitura do ficheiro
        } catch(RemoteException e) {
            System.out.println(e.getMessage());
        }catch(Exception e) { e.printStackTrace(); }

        String[] respostaBalancer = null;

        try {

            File clientpathfile = new File("exemploScript.scpt"); //abrir um ficheiro script qualquer com um conteúdo

            byte [] mydata = new byte[(int) clientpathfile.length()]; // criar um array de bytes para receber o conteúdo do ficheiro

            FileInputStream in=new FileInputStream(clientpathfile); // abrir o ficheiro em modo de leitura

            System.out.println("Uploading to balancer...");

            in.read(mydata, 0, mydata.length); //converter o conteúdo do ficheiro para um array de bytes. Também podiamos converter para base64, entre outros...

            respostaBalancer = balancerInterface.submetePedido(mydata, resposta); // chamar a interface que está implementada no balancer, que submete o pedido de processamento, devolvendo o id do pedido e o id do processador que o balanceador escolheu para concluir o pedido

            System.out.println("Upload made to balancer...");

            System.out.println("Request Id: " + respostaBalancer[0]);

            in.close(); // fecha o modo de leitura do ficheiro
        } catch(RemoteException e) {
            System.out.println(e.getMessage());
        }catch(Exception e) { e.printStackTrace(); }

        ProcessorInterface processorInterface = null;

        try {
            assert respostaBalancer != null;
            System.out.println("Getting state...");
            processorInterface = (ProcessorInterface) Naming.lookup(respostaBalancer[1]);// conectar ao rmi do processor

            System.out.println("Answer by processor: " + RequestState.valueOf(processorInterface.getEstado(respostaBalancer[0])).getValue());// vai buscar o estado do pedido ao processor. Aqui é utilizado uma class Enum 'RequestState' para associar a resposta do método a um estado em concreto

        } catch(RemoteException e) {
            System.out.println(e.getMessage());
        } catch(Exception e) { e.printStackTrace(); }

    }
}