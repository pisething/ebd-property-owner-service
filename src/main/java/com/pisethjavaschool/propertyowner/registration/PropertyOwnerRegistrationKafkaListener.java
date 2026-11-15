package com.pisethjavaschool.propertyowner.registration;

import com.pisethjavaschool.platform.events.registration.BusinessOwnerRegistrationRequested;
import com.pisethjavaschool.platform.events.registration.RegistrationEventTopics;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class PropertyOwnerRegistrationKafkaListener {

    private final PropertyOwnerRegistrationConsumer consumer;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = RegistrationEventTopics.REGISTRATION_REQUESTED,
            groupId = "property-owner-registration-v1")
    public void onRegistrationRequested(String payload) {
        BusinessOwnerRegistrationRequested event = objectMapper.readValue(
                payload, BusinessOwnerRegistrationRequested.class);
        consumer.process(event).block();
    }
}