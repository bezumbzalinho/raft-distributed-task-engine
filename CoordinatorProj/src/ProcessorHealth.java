public class ProcessorHealth {
    private final String address;
    private Long ultimaVez;

    public ProcessorHealth(String _address, Long _ultimaVez) {
        address = _address;
        ultimaVez = _ultimaVez;
    }
    public Long getUltimaVez() { return ultimaVez; }

    public String getAddress() {
        return address;
    }

    public void setUltimaVez(Long _ultimaVez) {
        ultimaVez = _ultimaVez;
    }
}
