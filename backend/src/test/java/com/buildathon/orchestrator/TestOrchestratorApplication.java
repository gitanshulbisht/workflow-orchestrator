package com.buildathon.orchestrator;

import org.springframework.boot.SpringApplication;

public class TestOrchestratorApplication {

	public static void main(String[] args) {
		SpringApplication.from(OrchestratorApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
