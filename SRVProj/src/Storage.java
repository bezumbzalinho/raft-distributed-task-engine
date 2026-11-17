import java.io.*;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;

public class Storage extends UnicastRemoteObject implements StorageInterface {
    private final Lock l1 = new Lock(), l2 = new Lock(); // l1 para a variavel numberOfClientsRequests, l2 para a variável numberOfVotes
    private final LockConsistency l3 = new LockConsistency();
    public String myAddress, raftState = "follower";
    public boolean obteveResposta = false;
    private Integer numberOfVotes = 0, numberOfClientsRequests = 0, numberofServers = 2;
    private final LinkedList<Thread> threadLinkedList = new LinkedList<>();

    protected Storage(String _address) throws RemoteException {
        myAddress = _address;

        MulticastReceiverRaft m = new MulticastReceiverRaft(this);
        m.start();

        sendVoteRequest(true);
    }

    public void sendVoteRequest(boolean intervalo) {
        Thread t = (new Thread(() -> {
            try {
                raftState = "candidate";
                numberOfVotes = 0;
                if(threadLinkedList.size() > 0) {
                    for (Thread th : threadLinkedList) {
                        th.stop();
                    }
                    threadLinkedList.clear();
                }
                if(intervalo) {
                    Random rand = new Random();
                    int interval = rand.nextInt(300, 700);
                    Thread.sleep(interval);
                }
                if(!obteveResposta) {
                    MulticastPublisherRaft multicastPublisherRaft = new MulticastPublisherRaft();
                    multicastPublisherRaft.multicast("votar|" + myAddress);
                    System.out.println("Mandei um pedido de voto para os meus companheiros e sou o: " + myAddress);
                } else if (!raftState.equals("leader")) {
                    raftState = "follower";
                    System.out.println("Como não sou o lider limito-me a ouvir msgs e sou o: " + myAddress);
                    keepListentoLeader();
                }

            } catch (InterruptedException | IOException e) {
                System.out.println("sendVoteRequest: " + e.getMessage());
            }
        }));
        t.start();
    }

    public void sendVoteResponse(String leaderAddress) throws NotBoundException, InterruptedException, IOException {
        StorageInterface pI = (StorageInterface) Naming.lookup(leaderAddress);

        pI.receiveVotes("leaderElection");
    }

    public void sendSetChange(String leaderAddress) throws NotBoundException, InterruptedException, IOException {
        StorageInterface pI = (StorageInterface) Naming.lookup(leaderAddress);

        pI.receiveVotes("setChange");
    }

    private void keepListentoLeader() {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    Random rand = new Random();
                    while (true) {
                        int interval = rand.nextInt(31*1000, 40*1000);
                        Thread.sleep(interval);
                        if(obteveResposta) {
                            obteveResposta = false;
                            System.out.println("Verifiquei que o lider tem mandado msgs e sou o: " + myAddress);
                        } else if (raftState.equals("follower")) {
                            numberofServers--;
                            System.out.println("Como o lider nao tem mandado msgs, entao vou-me candidatar a lider e sou o: " + myAddress);
                            if(numberofServers > 1) {
                                sendVoteRequest(false);
                            } else {
                                raftState = "leader";
                                sendContinuosMsgsToPartners();

                                BalancerInterface bI = (BalancerInterface) Naming.lookup("rmi://localhost:2023/balancer");
                                bI.setStorageLeader(myAddress);

                                System.out.println("Sou o lider: " + myAddress);
                            }
                            break;
                        }
                    }
                } catch (InterruptedException | MalformedURLException | NotBoundException | RemoteException e) {
                    System.out.println("keepListentoLeader: " + e.getMessage());
                }
            }
        };
        t.start();
        threadLinkedList.add(t);
    }

    private void sendContinuosMsgsToPartners() {
        MulticastPublisherRaft multicastPublisherRaft = new MulticastPublisherRaft();
        Thread t = (new Thread(() -> {
            try {
                while (raftState.equals("leader")) {
                    Thread.sleep(15*1000);
                    multicastPublisherRaft.multicast("atualizar|" + myAddress);
                    System.out.println("Mando mensagens aos meus companheiros para saberem que estou cá, e sou o: " + myAddress);
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }));
        t.start();
        threadLinkedList.add(t);
    }

    @Override
    public String uploadFileToServer(byte[] mybyte) throws IOException, InterruptedException {

        if(raftState.equals("leader")) {
            l1.lock();
            l2.lock();
            numberOfVotes = 0;
            l2.unlock();
            Integer variavelAntiga = numberOfClientsRequests;
            MulticastPublisherRaft mP = new MulticastPublisherRaft();
            mP.multicast("set|" + myAddress);
            l3.lock();
            //Thread.sleep(2*1000); // faz-se aqui um timeoutsinho para esperar até que o lider e os seus companheiros comuniquem
            if(variavelAntiga.equals(numberOfClientsRequests)) { // se a variável estiver igual quer dizer que que não houve consenso então tem que se libertar as Threads q estão à espera e retorna null alguma coisa deu mal
                l1.unlock();
                return null;
            }
        }

        String indentifier = "";

        UUID uuid = UUID.randomUUID(); // cria um indentificador único

        indentifier = uuid.toString();

        File serverpathfile = new File("storage/"); // abre a pasta que se criou para armazenar os ficheiros (lado do servidor)

        FileOutputStream out=new FileOutputStream(serverpathfile+"/"+indentifier); // cria o ficheiro, dentro da pasta, com o nome igual ao identificador criado atrás

        out.write(mybyte); // escreve no ficheiro o conteúdo que recebeu

        out.flush();
        out.close();

        System.out.println("Done writing data...");

        return indentifier;
    }

    /*
    * Este método, a partir do 'idFicheiro', procura se existe o ficheiro.
    * Se existir, retorna o conteúdo do ficheiro atravé dum array de bytes. Se
    * não, retorn null, o quer dizer que algo correu mal dentro deste método
    * */

    @Override
    public byte[] getFile(String idFicheiro) throws RemoteException {

        File file = new File("storage/" + idFicheiro);

        if(file.exists() && file.isFile()) {
            byte [] mydata = new byte[(int) file.length()];

            try {
                FileInputStream in = new FileInputStream(file);

                in.read(mydata, 0, mydata.length);

                return mydata;
            } catch (Exception e) {
                return null;
            }

        } else return null;

    }

    @Override
    public void submitOutput(byte[] mybyte, String idPedido) throws IOException {
        File serverpathfile = new File("storage/"); // abre a pasta que se criou para armazenar os ficheiros (lado do servidor)

        FileOutputStream out=new FileOutputStream(serverpathfile+"/"+idPedido); // cria o ficheiro, dentro da pasta, com o nome igual ao identificador criado atrás

        out.write(mybyte); // escreve no ficheiro o conteúdo que recebeu

        out.flush();
        out.close();
    }

    public void requestsUpdate(Integer nova) throws InterruptedException {
        l1.lock();
        System.out.println("Vou atualizar a variavel 'numberOfClientsRequests' para " + nova + " e sou o: " + myAddress);
        numberOfClientsRequests = nova;
        l1.unlock();
    }

    @Override
    public void receiveVotes(String type) throws NotBoundException, InterruptedException, IOException {
        if(type.equals("leaderElection")) {
            l2.lock();
            if(raftState.equals("leader")) {
                numberOfVotes = 0;
                l2.unlock();
                return;
            }
            numberOfVotes++;
            if((numberOfVotes.equals(numberofServers) || numberofServers/2 < numberOfVotes)) {
                raftState = "leader";
                sendContinuosMsgsToPartners();

                BalancerInterface bI = (BalancerInterface) Naming.lookup("rmi://localhost:2023/balancer");
                bI.setStorageLeader(myAddress);

                System.out.println("Sou o lider: " + myAddress);
            }
            l2.unlock();
        } else if (type.equals("setChange") && raftState.equals("leader")) {
            l2.lock();
            if((numberOfVotes.equals(numberofServers-1))) {
                l2.unlock();
                return;
            }
            numberOfVotes++;
            if((numberOfVotes.equals(numberofServers-1))) {
                MulticastPublisherRaft mP = new MulticastPublisherRaft();
                mP.multicast("commit|" + myAddress + "|" + (++numberOfClientsRequests));
                l3.unlock();
                l1.unlock();
                System.out.println("O lider '" + myAddress + "' confirmou a alteração da variavel 'numberOfClientsRequests' por consenso de todos dos seus companheiros. N votos: " + numberOfVotes);
            }
            l2.unlock();
        }
    }
}
