public class Main {
    public static void main(String[] args) {
        for (int i = 0; i < 2; i++)
            StorageServer.main(new String[]{Integer.toString(2020+i)}); //cria as 2 storages
    }
}