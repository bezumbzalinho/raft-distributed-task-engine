import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.MulticastSocket;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class MulticastReceiverRaft extends Thread {
    protected MulticastSocket socket = null;
    protected byte[] buf = new byte[256];
    private final Storage storage;

    public MulticastReceiverRaft(Storage _storage) {
        storage = _storage;
    }

    public void run() {
        try {
            socket = new MulticastSocket(7777);
            InetAddress group = InetAddress.getByName("232.0.0.0");
            socket.joinGroup(group);
            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String received = new String(
                        packet.getData(), 0, packet.getLength());

                String[] res = received.split("\\|");
                if(res[0].equals("votar")) {
                    storage.obteveResposta = true;
                    System.out.println("Vou votar no: '" + res[1] + "' e sou o: " + storage.myAddress);
                    storage.sendVoteResponse(res[1]);
                } else if (res[0].equals("atualizar") && !res[1].equals(storage.myAddress)) {
                    System.out.println("Recebi mensagem do lider: '" + res[1] + "' e sou o: " + storage.myAddress);
                    storage.obteveResposta = true;
                } else if (res[0].equals("set") && !res[1].equals(storage.myAddress)) {
                    storage.obteveResposta = true;
                    System.out.println("Recebi mensagem do lider: '" + res[1] + "' que há um novo pedido então vou-lhe responder que tem o meu voto para alterar a variável 'numberOfClientsRequests' e sou o: " + storage.myAddress);
                    storage.sendSetChange(res[1]);
                } else if (res[0].equals("commit") && !res[1].equals(storage.myAddress)) {
                    storage.obteveResposta = true;
                    System.out.println("Recebi mensagem do lider: '" + res[1] + "' para fazer o commit para alterar a variável 'numberOfClientsRequests' e sou o: " + storage.myAddress);
                    storage.requestsUpdate(Integer.parseInt(res[2]));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}