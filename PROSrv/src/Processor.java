import java.io.*;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Processor extends UnicastRemoteObject implements ProcessorInterface {

    private final Lock l1 = new Lock(), l2 = new Lock(), l3 = new Lock(), l4 = new Lock(); // l1 para requestState; l2 para requests; l3 para processorReplication; l4 para numberVotes
    private final HashMap<String, String> requestState = new HashMap<>();
    private final LinkedList<Requests> requests = new LinkedList<>();
    public String myAddress;
    private String replicaAddress;
    private final LockRequests threadP = new LockRequests();
    private Thread threadCoordinator;
    private Integer numberOfVotes = 0, numberofProcessors = 0;
    /* Esta lista fica com a informação do(s) processador(es) que este processador está a replicar. O seu endereço, script, idFicheiro e idPedido */
    private final LinkedList<ProcessorInfoReplication> processorReplication = new LinkedList<>();

    protected Processor(String _address) throws RemoteException {
        myAddress = _address;
        MulticastPublisher multicastPublisher = new MulticastPublisher();

        try {
            multicastPublisher.multicast("setup|" + myAddress + "|" + 0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        MulticastReceiverProcessors mP = new MulticastReceiverProcessors(this);
        mP.start();

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                l1.lock();
                long numberOrdersProcess = requestState.entrySet().stream().filter(x -> x.getValue().equals("processing")).count();
                l1.unlock();

                multicastPublisher.multicast("update|" + myAddress + "|" + numberOrdersProcess);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }, 15, 15, TimeUnit.SECONDS);

        Thread t = (new Thread(() -> {
            String idPedido = "";
            while (true) {
                try {
                    threadP.lock();
                    l2.lock();
                    Requests request = requests.getFirst();
                    idPedido = request.getIdPedido();
                    l2.unlock();
                    try {
                        execute(request.getScript(), request.getIdFicheiro(), request.getIdPedido());
                        l1.lock();
                        requestState.put(idPedido, "done");
                        l1.unlock();
                    } catch (IOException | NotBoundException e) {
                        l1.lock();
                        requestState.put(idPedido, "error");
                        l1.unlock();
                    } finally {
                        l2.lock();
                        requests.remove(request);
                        l2.unlock();
                        if(replicaAddress != null) {
                            /* Depois de concluir executação do script, este processador manda a nova atualização do pedido ao seu replicador */
                            ProcessorInterface processorInterface = (ProcessorInterface) Naming.lookup(replicaAddress);

                            processorInterface.removeReplication(idPedido);
                        } else {
                            /* Depois de concluir executação do script, este processador manda a nova atualização do pedido ao seu replicador */
                            CoordinatorInterface coordinatorInterface = (CoordinatorInterface) Naming.lookup("rmi://localhost:2022/coordinator");

                            if((replicaAddress = coordinatorInterface.getReplication(myAddress)) != null) {
                                ProcessorInterface pI = (ProcessorInterface) Naming.lookup(replicaAddress);
                                pI.removeReplication(request.getIdPedido());
                            }
                        }
                    }
                } catch (InterruptedException | NotBoundException | MalformedURLException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    try {
                        CoordinatorInterface coordinatorInterface = (CoordinatorInterface) Naming.lookup("rmi://localhost:2022/coordinator");

                        if((replicaAddress = coordinatorInterface.getReplication(myAddress)) != null) {
                            ProcessorInterface pI = (ProcessorInterface) Naming.lookup(replicaAddress);
                            pI.removeReplication(idPedido);
                        }
                    } catch (RemoteException | NotBoundException | MalformedURLException | InterruptedException eIn) {
                        eIn.printStackTrace();
                    }
                }
            }
        }));
        t.start();

        sendRequestsToCoordinator();
    }

    public void verifyCoordinatorState(String leaderAddress) {
        Thread t = new Thread(() -> {
            try {
                ProcessorInterface pI = (ProcessorInterface) Naming.lookup(leaderAddress);

                try {
                    Naming.lookup("rmi://localhost:2022/coordinator");

                    pI.receiveVotes(-1);
                } catch (RemoteException | MalformedURLException | NotBoundException exception) {
                    pI.receiveVotes(1);
                }
            } catch (NotBoundException | InterruptedException | IOException e) {
                //System.out.println("verifyCoordinatorState: " + e.getMessage());
                System.out.println("Foi detetada uma falha fail-stop quando um companheiro tentava comunicar com o processador que acusou falha no coordenador");
            }
        });
        t.start();
    }

    @Override
    public void submeteScript(byte[] script, String idFicheiro, String idPedido) throws RemoteException {
        Thread t = (new Thread(() -> {
            try {
                l1.lock();
                requestState.put(idPedido, "processing"); // acrescenta na estrutura HashMap o id do Pedido e o estado desse pedido. Para quando o cliente perguntar em que estado está um seu pedido, retornar o seu estado. O estado inicial é 'processing'
                l1.unlock();

                l2.lock();
                requests.add(new Requests(script, idPedido, idFicheiro)); // acrescenta numa LinkedList os Scripts recebidos
                l2.unlock();

                threadP.unlock();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        t.start();
    }

    private void execute(byte[] script, String idFicheiro, String idPedido) throws NotBoundException, InterruptedException, IOException {

            StorageInterface storageInterface = (StorageInterface) Naming.lookup("rmi://localhost:2020/storage");

            byte[] dataReceived = storageInterface.getFile(idFicheiro);

            if(dataReceived == null) throw new RuntimeException();

            File clienteFile = fileCreator("destdir/" + idFicheiro, dataReceived);

            l2.lock();
            File scriptFile = fileCreator("destdir/scriptReceiver.scpt", script);
            l2.unlock();

            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(new String[]{scriptFile.getAbsolutePath(), clienteFile.getAbsolutePath()});

            proc.waitFor();

            if(proc.exitValue() != 0) throw new RuntimeException();

            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));

            String line;
            StringBuilder resultadoScript = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                resultadoScript.append(line).append("\n");
            }

            clienteFile.delete();

            storageInterface.submitOutput(resultadoScript.substring(0, resultadoScript.length()-1).getBytes(), idPedido);

    }

    /*
    * Este método privado cria ficheiros temporários para o conteúdo do ficheiro que o
    * processor recebe da storage e, também, para o script que o cliente envia.
    * Depois esses ficheiros são eliminados
    * */
    private File fileCreator(String pathName, byte[] dataReceived) throws IOException {
        File file = new File(pathName);

        FileOutputStream out = new FileOutputStream(file);

        out.write(dataReceived);

        out.flush();
        out.close();

        return file;
    }

    /*
     * notfound = Not Found
     * processing = Processing
     * done = Done
     * error = Error
     * */

    @Override
    public String getEstado(String idPedido) throws RemoteException, InterruptedException {
        l1.lock();
        String resultado = requestState.getOrDefault(idPedido, "notfound"); // devolve ao cliente o estado dum seu pedido. Se não encontrar devolve notfound, que quer dizer que não encontrou
        l1.unlock();
        return resultado;
    }

    @Override
    public void submeteScriptReplica(byte[] script, String idFicheiro, String idPedido, String address) throws RemoteException {
        Thread t = (new Thread(() -> {
            try {
                l3.lock();
                processorReplication.add(new ProcessorInfoReplication(script, address, idPedido, idFicheiro));
                l3.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        t.start();
    }

    @Override
    public void resumeTasks(String deadProcessor, String ReplicationProcessor) throws RemoteException {
        Thread t = (new Thread(() -> {
            try {
                // ir buscar todos os pedidos pendentes no replicador
                ProcessorInterface processorInterface = (ProcessorInterface) Naming.lookup(ReplicationProcessor);

                LinkedList<Requests> lista = processorInterface.returnAllRequestsFromDeadProcessor(deadProcessor);

                for (Requests k : lista) {
                    l1.lock();
                    requestState.put(k.getIdPedido(), "processing"); // acrescenta na estrutura HashMap o id do Pedido e o estado desse pedido. Para quando o cliente perguntar em que estado está um seu pedido, retornar o seu estado. O estado inicial é 'processing'
                    l1.unlock();

                    l2.lock();
                    requests.add(k); // acrescenta numa LinkedList os Scripts recebidos
                    l2.unlock();
                }
                threadP.unlock();
            } catch (InterruptedException | MalformedURLException | NotBoundException | RemoteException e ) {
                e.printStackTrace();
            }
        }));
        t.start();
    }

    @Override
    public void removeReplication(String idPedido) throws RemoteException {
        Thread t = (new Thread(() -> {
            try {
                l3.lock();
                for (int i = 0; i < processorReplication.size(); i++)
                    if (processorReplication.get(i).getIdPedido().equals(idPedido)) {
                        processorReplication.remove(i);
                        break;
                    }
                l3.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        t.start();
    }

    @Override
    public void putReplicaAddress(String address) throws RemoteException {
        replicaAddress = address;
    }

    @Override
    public LinkedList<Requests> returnAllRequestsFromDeadProcessor(String address) throws RemoteException, InterruptedException {
        LinkedList<Requests> listaPedidos = new LinkedList<>();

        LinkedList<Integer> requestsToRemove = new LinkedList<>();

        l3.lock();
        for (int i = 0; i < processorReplication.size(); i++)
            if (processorReplication.get(i).getAddress().equals(address)) {
                requestsToRemove.add(i);
                listaPedidos.add(new Requests(processorReplication.get(i).getScript(), processorReplication.get(i).getIdPedido(), processorReplication.get(i).getIdFicheiro()));
            }
        l3.unlock();

        Thread t = (new Thread(() -> {
            try {
                l3.lock();
                for (Integer i : requestsToRemove)
                    processorReplication.remove(i);
                l3.unlock();
                requestsToRemove.clear();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        t.start();

        return listaPedidos;
    }

    public void stopRequestsToCoordinator() {
        threadCoordinator.stop();
    }

    private void sendRequestsToCoordinator() {
        threadCoordinator = new Thread() {
            @Override
            public void run() {
                try {
                    sleep(1000); // garantir que todos os processadores estão criados
                    Random random = new Random();
                    while (true) {
                        try {
                            CoordinatorInterface cI = (CoordinatorInterface) Naming.lookup("rmi://localhost:2022/coordinator");
                            numberofProcessors = cI.getCountProcessors();
                            System.out.println("Perguntei ao coodenador quantos processadores havia: " + numberofProcessors + " e sou o: " + myAddress);
                        } catch (RemoteException | InterruptedException | NotBoundException e) {
                            /* O coordenador não está ativo, por isso vou avisar os meus companheiros */
                            numberOfVotes = 0;
                            MulticastPublisherProcessors multicastPublisherRaft = new MulticastPublisherProcessors();
                            multicastPublisherRaft.multicast("failed|" + myAddress);
                            System.out.println("O coordenador não está ativo, por isso vou avisar os meus companheiros para saber o veredicto e sou o: " + myAddress);
                        } finally {
                            int interval = random.nextInt(45*1000, 60*1000);
                            sleep(interval);
                        }
                    }
                } catch (InterruptedException | IOException e) {
                    System.out.println("Coodinator:" + e.getMessage());
                }
            }
        };
        threadCoordinator.start();
    }

    @Override
    public void receiveVotes(Integer inc) throws IOException, InterruptedException {
        l4.lock();
        if((numberOfVotes.equals(numberofProcessors) || numberofProcessors/2 < numberOfVotes)) {
            l4.unlock();
            return;
        }
        numberOfVotes+=inc;
        if((numberOfVotes.equals(numberofProcessors) || numberofProcessors/2 < numberOfVotes)) {
            threadCoordinator.stop();
            MulticastPublisherProcessors mP = new MulticastPublisherProcessors();
            mP.multicast("resetCoord|" + myAddress);
            System.out.println("O '" + myAddress + "' confirmou falha do coordenador após consenso da maioria dos seus companheiros. N votos: " + numberOfVotes);
        }
        l4.unlock();
    }
}
