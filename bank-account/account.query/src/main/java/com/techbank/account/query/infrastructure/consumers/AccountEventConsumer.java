package com.techbank.account.query.infrastructure.consumers;

import com.techbank.account.query.infrastructure.handlers.EventHandler;
import com.techbank.cqrs.core.events.BaseEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.logging.Logger;

@Service
public class AccountEventConsumer implements EventConsumer {
    private final Logger logger = Logger.getLogger(AccountEventConsumer.class.getName());

    @Autowired
    private EventHandler eventHandler;

    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Override
    public void consume(@Payload BaseEvent event, Acknowledgment ack) {
        try {
            logger.info(MessageFormat.format("Consumed {0} - account: {1}, version: {2}",
                    event.getClass().getSimpleName(), event.getId(), event.getVersion()));
            var eventHandlerMethod = eventHandler.getClass().getDeclaredMethod("on", event.getClass());
            eventHandlerMethod.setAccessible(true);
            eventHandlerMethod.invoke(eventHandler, event);
            ack.acknowledge();
        } catch (Exception e) {
            throw new RuntimeException("Error while consuming event", e);
        }
    }
}
