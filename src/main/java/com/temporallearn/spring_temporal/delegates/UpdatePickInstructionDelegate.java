package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.dto.UpdatePickInstructionDto;
import com.temporallearn.spring_temporal.dto.UpdatePickInstructionResult;
import com.temporallearn.spring_temporal.service.PickInstructionService;
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

        PickInstruction instruction = objectMapper.readValue(instructionJson, PickInstruction.class);
        TransactionUpdate transactionUpdate = objectMapper.readValue(transactionUpdateJson, TransactionUpdate.class);

        log.info("UpdatePickInstructionDelegate executing for pickId: {}, transactionId: {}",
                instruction.getPickId(), transactionUpdate.getTransactionId());

        // Build the DTO the same way PickInstructionWorkflowImpl does
        UpdatePickInstructionDto updateDto = buildUpdateDto(instruction, transactionUpdate.getTransactionId());

        UpdatePickInstructionResult updateResult = pickInstructionService.updatePickInstruction(updateDto);

        log.info("Update result — success: {}, retriable: {}, status: {}, errorCode: {}",
                updateResult.isSuccess(), updateResult.isRetriable(),
                updateResult.getStatus(), updateResult.getErrorCode());

        execution.setVariable("updateSuccess", updateResult.isSuccess());
        execution.setVariable("updateRetriable", updateResult.isRetriable());

        if (!updateResult.isSuccess()) {
            // Store error details for MarkFailureInButlerCoreDelegate (non-retriable path)
            execution.setVariable("updateErrorCode", updateResult.getErrorCode());
            execution.setVariable("updateErrorMessage", updateResult.getMessage());

            if (!updateResult.isRetriable()) {
                // Non-retriable: set final status variables before the terminal path runs
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
                instruction.getPickId(), transactionUpdate);

        execution.setVariable("txComplete", txComplete);

        if (txComplete) {
            execution.setVariable("finalStatus", "COMPLETED");
        }
    }

    private UpdatePickInstructionDto buildUpdateDto(PickInstruction instruction, String transactionId) {
        return UpdatePickInstructionDto.builder()
                .orderId(instruction.getPickId())
                .transactionId(transactionId)
                .ppsId(instruction.getPpsId())
                .ppsbinId(UpdatePickInstructionDto.PpsBinIdDto.builder()
                        .ppsId(instruction.getPpsId())
                        .binId(instruction.getBinId() != null ? instruction.getBinId() : "")
                        .build())
                .ppsPoint(instruction.getPpsPoint())
                .seatName(instruction.getSeatName())
                .slotref(instruction.getSlotref())
                .userLoggedIn(instruction.getUserLoggedIn())
                .isMarkedContainerScanned(instruction.isMarkedContainerScanned())
                .build();
    }
}
