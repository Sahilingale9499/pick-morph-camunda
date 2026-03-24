package com.butler.aeorder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class SpringCamundaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCamundaApplication.class, args);
	}

}
