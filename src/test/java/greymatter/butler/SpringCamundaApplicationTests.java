package greymatter.butler;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Disabled("Requires live PostgreSQL + Zeebe broker — run via ./setup.sh")
class SpringCamundaApplicationTests {

	@Test
	void contextLoads() {
	}

}
