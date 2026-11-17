import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastReceiver extends Thread {
    protected MulticastSocket socket = null;
    protected byte[] buf = new byte[256];
    private final Coordinator coordinator;
    protected MulticastReceiver(Coordinator _coordinator) {
        coordinator = _coordinator;
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
                //if ("end".equals(received)) break;
                String[] res = received.split("\\|");
                coordinator.l1.lock();
                if(res[0].equals("setup")) {
                    coordinator.createProcessor(res[1]);
                } else if(res[0].equals("update")) {
                    for (ProcessorHealth i : coordinator.processors)
                        if(i.getAddress().equals(res[1])) {
                            i.setUltimaVez(System.currentTimeMillis());
                            break;
                        }
                }
                coordinator.l1.unlock();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}