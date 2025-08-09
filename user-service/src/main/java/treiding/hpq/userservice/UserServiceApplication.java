package treiding.hpq.userservice;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;

@SpringBootApplication
public class UserServiceApplication {

	public static void main(String[] args) {
		File envFile = new File("user-service/.env");
		if (envFile.exists()) {
			Dotenv dotenv = Dotenv.configure()
					.directory("user-service")
					.filename(".env")
					.load();
			dotenv.entries().forEach(entry ->
					System.setProperty(entry.getKey(), entry.getValue())
			);
		}
		SpringApplication.run(UserServiceApplication.class, args);
	}

}
