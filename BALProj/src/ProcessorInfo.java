public class ProcessorInfo {
    private String address;
    private Integer carga;

    public ProcessorInfo(String _address, Integer _carga) {
        address = _address;
        carga = _carga;
    }

    public Integer getCarga() {
        return carga;
    }

    public String getAddress() {
        return address;
    }

    public void setCarga(Integer _carga) { carga = _carga; }
}
