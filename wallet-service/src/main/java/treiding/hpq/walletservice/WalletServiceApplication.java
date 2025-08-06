package treiding.hpq.walletservice;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;

@SpringBootApplication
public class WalletServiceApplication {

	public static void main(String[] args) {
		File envFile = new File("wallet-service/.env");
		if (envFile.exists()) {
			Dotenv dotenv = Dotenv.configure()
					.directory("wallet-service")
					.filename(".env")
					.load();
			dotenv.entries().forEach(entry ->
					System.setProperty(entry.getKey(), entry.getValue())
			);
		}
		SpringApplication.run(WalletServiceApplication.class, args);
	}

}
