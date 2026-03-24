package com.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import com.butler.aeorder.dto.TransactionUpdate;
import com.butler.aeorder.dto.UpdatePickInstructionDto;
import com.butler.aeorder.dto.UpdatePickInstructionResult;
import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Builds the UpdatePickInstructionDto from process variables, calls Butler Core,
 * processes the transaction update, and sets outcome variables:
 * - updateSuccess (Boolean)
 * - updateRetriable (Boolean)
 * - txComplete (Boolean)
 * - finalStatus / finalFailureReason (on non-retriable failure)
 * - updateErrorCode / updateErrorMessage (for MarkFailureInButlerCoreDelegate)
 */
@Component
@Slf4j
public class UpdatePickInstructionDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public UpdatePickInstructionDelegate(PickInstructionService pickInstructionService,
                                          ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String instructionJson = (String) execution.getVariable("instructionJson");
        String transactionUpdateJson = (String) execution.getVariable("transactionUpdateJson");

        PickInstructionRequestMessage msg = objectMapper.readValue(instructionJson, PickInstructionRequestMessage.class);
        TransactionUpdate transactionUpdate = objectMapper.readValue(transactionUpdateJson, TransactionUpdate.class);

        log.info("UpdatePickInstructionDelegate executing for pickId: {}, transactionId: {}",
                msg.getId(), transactionUpdate.getTransactionId());

        UpdatePickInstructionDto updateDto = buildUpdateDto(msg, transactionUpdate);
        UpdatePickInstructionResult updateResult = pickInstructionService.updatePickInstruction(updateDto);

        log.info("Update result — success: {}, retriable: {}, status: {}, errorCode: {}",
                updateResult.isSuccess(), updateResult.isRetriable(),
                updateResult.getStatus(), updateResult.getErrorCode());

        execution.setVariable("updateSuccess", updateResult.isSuccess());
        execution.setVariable("updateRetriable", updateResult.isRetriable());

        if (!updateResult.isSuccess()) {
            execution.setVariable("updateErrorCode", updateResult.getErrorCode());
            execution.setVariable("updateErrorMessage", updateResult.getMessage());

            if (!updateResult.isRetriable()) {
                String failureReason = "Butler Core error [" + updateResult.getErrorCode() + "]: "
                        + updateResult.getMessage();
                execution.setVariable("finalStatus", "FAILED");
                execution.setVariable("finalFailureReason", failureReason);
                execution.setVariable("failureReason", failureReason);
            }

            execution.setVariable("txComplete", false);
            return;
        }

        // SUCCESS — process the transaction update and determine completion
        boolean txComplete = pickInstructionService.processTransactionUpdate(
                msg.getId(), transactionUpdate);

        execution.setVariable("txComplete", txComplete);

        if (txComplete) {
            execution.setVariable("finalStatus", "COMPLETED");
        }
    }

    /**
     * Build UpdatePickInstructionDto from the Kafka message fields + transaction update data.
     * slotref comes from the message (primary source for Kafka-driven flow).
     * ppsbinId is built from message ppsId + binId.
     */
    private UpdatePickInstructionDto buildUpdateDto(PickInstructionRequestMessage msg,
                                                     TransactionUpdate transactionUpdate) {
        return UpdatePickInstructionDto.builder()
                .orderId(msg.getId())
                .transactionId(transactionUpdate.getTransactionId())
                .ppsId(msg.getPpsId())
                .ppsbinId(UpdatePickInstructionDto.PpsBinIdDto.builder()
                        .ppsId(msg.getPpsId())
                        .binId(msg.getBinId() != null ? msg.getBinId() : "")
                        .build())
                .ppsPoint(null)        // not in Kafka message — TODO: add when butler_server provides it
                .seatName(null)        // not in Kafka message — TODO
                .slotref(msg.getSlotId())
                .userLoggedIn(null)    // not in Kafka message — TODO
                .isMarkedContainerScanned(false)
                .build();
    }
}
