package treiding.hpq.matchingservice;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;

import java.io.File;


@EntityScan(basePackages = {"treiding.hpq.basedomain.entity"})
@SpringBootApplication(exclude = {
		org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class,
		org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class
})
public class MatchingServiceApplication {

	public static void main(String[] args) {
		File envFile = new File("matching-service/.env");
		if (envFile.exists()) {
			Dotenv dotenv = Dotenv.configure()
					.directory("matching-service")
					.filename(".env")
					.load();
			dotenv.entries().forEach(entry ->
					System.setProperty(entry.getKey(), entry.getValue())
			);
		}
		SpringApplication.run(MatchingServiceApplication.class, args);
	}

}
