import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastReceiver extends Thread {
    protected MulticastSocket socket = null;
    protected byte[] buf = new byte[256];
    private final Balancer balancer;
    protected MulticastReceiver(Balancer _balancer) {
            balancer = _balancer;
    }

    public void run() {
        try {
            socket = new MulticastSocket(4446);
            InetAddress group = InetAddress.getByName("230.0.0.0");
            socket.joinGroup(group);
            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String received = new String(
                        packet.getData(), 0, packet.getLength());
                String[] res = received.split("\\|");

                if(res[0].equals("update")) {
                    balancer.l1.lock(); // thread safe
                    for (ProcessorInfo i : balancer.processorsIdentifier)
                        if(i.getAddress().equals(res[1])) {
                            i.setCarga(Integer.parseInt(res[2]));
                            break;
                        }
                    balancer.l1.unlock();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}