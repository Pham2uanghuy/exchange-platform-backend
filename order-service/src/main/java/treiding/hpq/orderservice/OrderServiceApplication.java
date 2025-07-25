package treiding.hpq.orderservice;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;

@SpringBootApplication
@EntityScan(basePackages = {"treiding.hpq.basedomain.entity",
		"treiding.hpq.orderservice.outbox",})
@EnableScheduling
public class OrderServiceApplication {

	public static void main(String[] args) {
		File envFile = new File("order-service/.env");
		if (envFile.exists()) {
			Dotenv dotenv = Dotenv.configure()
					.directory("order-service")
					.filename(".env")
					.load();
			dotenv.entries().forEach(entry ->
					System.setProperty(entry.getKey(), entry.getValue())
			);
		}
		SpringApplication.run(OrderServiceApplication.class, args);
	}

}
