public enum RequestState {
    notfound("Not Found"),
    processing("Processing"),
    done("Done"),
    error("Error");

    private String value;
    RequestState(String valueReceived) {
        value = valueReceived;
    }

    public String getValue(){
        return value;
    }
}