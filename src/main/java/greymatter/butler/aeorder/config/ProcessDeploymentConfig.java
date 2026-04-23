package greymatter.butler.aeorder.config;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.response.DeploymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Deploys BPMN process definitions to Zeebe on application startup.
 *
 * Runs after the Spring context is fully initialized (via ApplicationRunner).
 * Fails fast — an exception here aborts startup so misconfigured deployments
 * are caught immediately rather than at runtime.
 */
@Configuration
@Slf4j
public class ProcessDeploymentConfig implements ApplicationRunner {

    private final CamundaClient camundaClient;

    public ProcessDeploymentConfig(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    @Override
    public void run(ApplicationArguments args) {
        int maxAttempts = 5;
        long delayMs = 5_000;
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                log.info("Deploying BPMN process definitions to Zeebe (attempt {}/{})...", attempt, maxAttempts);
                DeploymentEvent result = camundaClient.newDeployResourceCommand()
                        .addResourceFromClasspath("pick-instruction-workflow.bpmn")
                        .send()
                        .join();
                result.getProcesses().forEach(p ->
                        log.info("Deployed process: {} (version {}, key {})",
                                p.getBpmnProcessId(), p.getVersion(), p.getProcessDefinitionKey()));
                return;
            } catch (Exception e) {
                lastException = e;
                if (attempt < maxAttempts) {
                    log.warn("BPMN deployment attempt {}/{} failed — retrying in {}ms: {}",
                            attempt, maxAttempts, delayMs, e.getMessage());
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during BPMN deployment retry", ie);
                    }
                    delayMs = Math.min(delayMs * 2, 30_000);
                }
            }
        }
        throw new RuntimeException("Failed to deploy BPMN after " + maxAttempts + " attempts", lastException);
    }
}
