public class LockConsistency {
    public synchronized void lock() throws InterruptedException {
        wait(2000);
    }

    public synchronized void unlock() {
        notify();
    }
}
