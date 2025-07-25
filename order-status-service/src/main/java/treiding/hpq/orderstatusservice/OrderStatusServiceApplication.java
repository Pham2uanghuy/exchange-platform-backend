package treiding.hpq.orderstatusservice;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;

import java.io.File;

@SpringBootApplication
@EntityScan(basePackages = {"treiding.hpq.basedomain.entity"})
public class OrderStatusServiceApplication {
	public static void main(String[] args) {
		File envFile = new File("order-status-service/.env");
		if (envFile.exists()) {
			Dotenv dotenv = Dotenv.configure()
					.directory("order-status-service")
					.filename(".env")
					.load();
			dotenv.entries().forEach(entry ->
					System.setProperty(entry.getKey(), entry.getValue())
			);
		}
		SpringApplication.run(OrderStatusServiceApplication.class, args);
	}

}
