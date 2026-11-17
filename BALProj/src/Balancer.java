import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Balancer extends UnicastRemoteObject implements BalancerInterface {

    public LinkedList<ProcessorInfo> processorsIdentifier = new LinkedList<>();
    public Lock l1 = new Lock(); // l1 para processorsIdentifier
    private String storageLeader = "";

    protected Balancer() throws RemoteException {

        MulticastReceiver multicastReceiver = new MulticastReceiver(this);
        multicastReceiver.start();
    }

    @Override
    public String[] submetePedido(byte[] script, String idFicheiro) throws RemoteException, InterruptedException, NotBoundException, MalformedURLException {
        final String idPedido = UUID.randomUUID().toString();
        final String[] processadorEscolhido = { "" };

        final Integer[] _cargaMin = { processorsIdentifier.get(0).getCarga() };
        processadorEscolhido[0] = processorsIdentifier.get(0).getAddress();
        l1.lock();
        processorsIdentifier.forEach(processor -> {
            if(processor.getCarga() < _cargaMin[0]) {
                processadorEscolhido[0] = processor.getAddress();
                _cargaMin[0] = processor.getCarga();
            }
        });
        l1.unlock();

        ProcessorInterface processorInterface  = (ProcessorInterface) Naming.lookup(processadorEscolhido[0]); // conectar ao rmi de um processor

        processorInterface.submeteScript(script, idFicheiro, idPedido); // chamar a interface que está implementada no processor, que trata o script enviado e cria um pedido

        Thread t = (new Thread(() -> {
            try {
                if(processorsIdentifier.size() > 1) {
                    CoordinatorInterface cIR = (CoordinatorInterface) Naming.lookup("rmi://localhost:2022/coordinator"); // conectar ao rmi de um processor replica

                    cIR.replication(script, idFicheiro, idPedido, processadorEscolhido[0]);
                }
            } catch (MalformedURLException | RemoteException | NotBoundException e) {
                System.out.println("Foi detetada um falha fail-stop quando o balanceador tentava comunicar com o coordenador");
            }
        }));
        t.start();
        return new String[]{idPedido, processadorEscolhido[0]}; // no indice 0 vai o Id do Pedido; no indice 1 vai o Id do processo
    }

    @Override
    public void addProcessor(String processorAddress) throws RemoteException {
        Thread t = (new Thread(() -> {
            try {
                l1.lock();
                processorsIdentifier.add(new ProcessorInfo(processorAddress, 0)); // acrescenta na estrutura LinkedList o identificador do processador
                l1.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        t.start();
    }

    @Override
    public void removeProcessor(String processorAddress) throws RemoteException {
        Thread t = (new Thread(() -> {
            try {
                l1.lock();
                for (int i = 0; i < processorsIdentifier.size(); i++)
                    if(processorsIdentifier.get(i).getAddress().equals(processorAddress)) {
                        processorsIdentifier.remove(i);
                        break;
                    }
                l1.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        t.start();
    }
    @Override
    public String getProcessorMoreFree() throws RemoteException, InterruptedException {
        final String[] processadorEscolhido = { "" };

        final Integer[] _cargaMin = { processorsIdentifier.get(0).getCarga() };
        processadorEscolhido[0] = processorsIdentifier.get(0).getAddress();

        l1.lock();
        processorsIdentifier.forEach(processor -> {
            if(processor.getCarga() < _cargaMin[0]) {
                processadorEscolhido[0] = processor.getAddress();
                _cargaMin[0] = processor.getCarga();
            }
        });
        l1.unlock();

        return processadorEscolhido[0];
    }

    @Override
    public void setStorageLeader(String _storageLeader) throws RemoteException {
        //System.out.println(_storageLeader);
        storageLeader = _storageLeader;
    }

    @Override
    public String getStorageLeader() throws RemoteException {
        return storageLeader;
    }
}
