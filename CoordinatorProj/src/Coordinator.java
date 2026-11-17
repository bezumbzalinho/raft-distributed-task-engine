import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Coordinator extends UnicastRemoteObject implements CoordinatorInterface {
    public LinkedList<ProcessorHealth> processors = new LinkedList<>();
    public Lock l1 = new Lock();
    protected Coordinator() throws RemoteException {
        MulticastReceiver multicastReceiver = new MulticastReceiver(this);
        multicastReceiver.start();
        LinkedList<Integer> processorsToRemove = new LinkedList<>();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                long tempoAtual = System.currentTimeMillis();
                l1.lock();
                for (int i = 0; i < processors.size(); i++)
                    if(tempoAtual - processors.get(i).getUltimaVez() > 30*1000) {
                        try {
                            removeProcessor(processors.get(i).getAddress());
                            processorsToRemove.add(i);
                        } catch (RemoteException | NotBoundException | MalformedURLException e) {
                            System.out.println("Foi detetada uma falha fail-stop quando o coordenador tentava comunicar com o balanceador e o processador escolhido para ficar com as replicas de outro");
                        }
                    }
                for (Integer i : processorsToRemove)
                    processors.remove(i);
                l1.unlock();
                processorsToRemove.clear();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public void createProcessor(String processorAddress) throws RemoteException, NotBoundException, MalformedURLException {
        BalancerInterface balancerInterface = (BalancerInterface) Naming.lookup("rmi://localhost:2023/balancer"); // conectar ao rmi do balancer

        balancerInterface.addProcessor(processorAddress);

        processors.add(new ProcessorHealth(processorAddress, System.currentTimeMillis()));
    }

    private void removeProcessor(String processorAddress) throws RemoteException, NotBoundException, MalformedURLException, InterruptedException {
        int hasCode1 = processorAddress.hashCode();

        l1.lock();
        if(processors.size() <= 1) return;
        int min = hasCode1 - processors.get(0).getAddress().hashCode();

        String processorReplication = processors.get(0).getAddress();

        for (int i = 1; i < processors.size(); i++)
            if(hasCode1 - processors.get(i).getAddress().hashCode() < min) {
                min = hasCode1 - processors.get(i).getAddress().hashCode();
                processorReplication = processors.get(i).getAddress();
                break;
            }
        l1.unlock();

        BalancerInterface balancerInterface = (BalancerInterface) Naming.lookup("rmi://localhost:2023/balancer"); // conectar ao rmi do balancer

        // remover o processador que morreu
        balancerInterface.removeProcessor(processorAddress);

        // ir buscar o processador mais livre
        String processorChosen = balancerInterface.getProcessorMoreFree();

        ProcessorInterface processorInterface = (ProcessorInterface) Naming.lookup(processorChosen);

        // submeter os novos pedidos para o processador escolhido
        processorInterface.resumeTasks(processorAddress, processorReplication);
    }

    @Override
    public void replication(byte[] script, String idFicheiro, String idPedido, String processorAddress) throws RemoteException {
        Thread t = (new Thread(() -> {
            try {
                int hasCode = processorAddress.hashCode();

                int min = hasCode - processors.get(0).getAddress().hashCode();

                String processorReplication = processors.get(0).getAddress();

                for (int i = 1; i < processors.size(); i++)
                    if(hasCode - processors.get(i).getAddress().hashCode() < min) {
                        min = hasCode - processors.get(i).getAddress().hashCode();
                        processorReplication = processors.get(i).getAddress();
                        break;
                    }
                ProcessorInterface pIR = (ProcessorInterface) Naming.lookup(processorReplication); // conectar ao rmi de um processor replica

                pIR.submeteScriptReplica(script, idFicheiro, idPedido, processorAddress);
            } catch (MalformedURLException | NotBoundException | RemoteException e) {
                System.out.println("Foi detetada uma falha fail-stop quando o coordenador tentava comunicar com o processador escolhido para replicar o que morreu");
            }
        }));
        t.start();
    }

    @Override
    public String getReplication(String processorAddress) throws RemoteException, InterruptedException {
        int hasCode1 = processorAddress.hashCode();

        l1.lock();
        if(processors.size() <= 1){
            l1.unlock();
            return null;
        }
        int min = hasCode1 - processors.get(0).getAddress().hashCode();

        String processorReplication = processors.get(0).getAddress();

        for (int i = 1; i < processors.size(); i++)
            if(hasCode1 - processors.get(i).getAddress().hashCode() < min) {
                min = hasCode1 - processors.get(i).getAddress().hashCode();
                processorReplication = processors.get(i).getAddress();
                break;
            }
        l1.unlock();

        return processorReplication;
    }

    @Override
    public Integer getCountProcessors() throws RemoteException, InterruptedException {
        l1.lock();
        Integer count = processors.size();
        l1.unlock();
        return count;
    }
}
