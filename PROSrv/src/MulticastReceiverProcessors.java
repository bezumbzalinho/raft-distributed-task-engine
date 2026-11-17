import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastReceiverProcessors extends Thread {
    protected MulticastSocket socket = null;
    protected byte[] buf = new byte[256];
    private final Processor processor;

    public MulticastReceiverProcessors(Processor _processor) {
        processor = _processor;
    }

    public void run() {
        try {
            socket = new MulticastSocket(8888);
            InetAddress group = InetAddress.getByName("231.0.0.0");
            socket.joinGroup(group);
            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String received = new String(
                        packet.getData(), 0, packet.getLength());

                String[] res = received.split("\\|");
                if (res[0].equals("failed")) {
                    System.out.println("Recebi um aviso por parte do companheiro '" + res[1] + "' que o coordenador ficou inativo, então vou verficar e sou o: " + processor.myAddress);
                    processor.verifyCoordinatorState(res[1]);
                } else if (res[0].equals("resetCoord") && !res[1].equals(processor.myAddress)) {
                    processor.stopRequestsToCoordinator();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}