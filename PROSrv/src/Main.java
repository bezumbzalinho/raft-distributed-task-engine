public class Main {
    public static void main(String[] args) {
        for (int i = 0; i < 3; i++)
            ProcessorServer.main(new String[]{Integer.toString(2024+i)}); //cria os 3 registos para os clientes e o balanceador acederem aos processors, que vão correr no porto desde 2024 até 2026
    }
}