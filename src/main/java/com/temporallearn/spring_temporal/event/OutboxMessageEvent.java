package com.temporallearn.spring_temporal.event;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class OutboxMessageEvent extends ApplicationEvent {

    private final Long outboxEventId;

    public OutboxMessageEvent(Object source, Long outboxEventId) {
        super(source);
        this.outboxEventId = outboxEventId;
    }
}
