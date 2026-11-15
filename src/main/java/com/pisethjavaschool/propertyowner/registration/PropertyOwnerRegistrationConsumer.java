package com.pisethjavaschool.propertyowner.registration;

import com.pisethjavaschool.platform.events.registration.BusinessOwnerRegistrationRequested;
import com.pisethjavaschool.platform.events.registration.PropertyOwnerProvisioned;
import com.pisethjavaschool.platform.events.registration.RegistrationEventTopics;
import com.pisethjavaschool.platform.outbox.R2dbcOutboxWriter;
import com.pisethjavaschool.platform.security.PlatformAuditorContext;
import com.pisethjavaschool.propertyowner.dto.CreatePropertyOwnerRequest;
import com.pisethjavaschool.propertyowner.dto.PropertyOwnerResponse;
import com.pisethjavaschool.propertyowner.enums.OwnerType;
import com.pisethjavaschool.propertyowner.facade.CreatePropertyOwnerFacade;
import com.pisethjavaschool.propertyowner.repository.PropertyOwnerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PropertyOwnerRegistrationConsumer {

    private static final UUID SYSTEM_AUDITOR = UUID.fromString("00000000-0000-0000-0000-000000000001");

    private final DatabaseClient databaseClient;
    private final PropertyOwnerRepository ownerRepository;
    private final CreatePropertyOwnerFacade createOwnerFacade;
    private final R2dbcOutboxWriter outboxWriter;
    private final ObjectMapper objectMapper;

    @Transactional
    public Mono<Void> process(BusinessOwnerRegistrationRequested request) {
        return Mono.defer(() -> {
            validate(request);
            Mono<Void> operation = claim(request.eventId())
                    .flatMap(claimed -> claimed ? provisionAndPublish(request) : Mono.empty());
            return PlatformAuditorContext.withAuditorId(operation, SYSTEM_AUDITOR);
        });
    }

    private Mono<Boolean> claim(UUID eventId) {
        return databaseClient.sql("""
                        INSERT INTO property_owner_registration_inbox (source_event_id)
                        VALUES (:eventId)
                        ON CONFLICT (source_event_id) DO NOTHING
                        RETURNING id
                        """)
                .bind("eventId", eventId)
                .map((row, metadata) -> row.get("id", UUID.class))
                .one()
                .map(id -> true)
                .defaultIfEmpty(false);
    }

    private Mono<Void> provisionAndPublish(BusinessOwnerRegistrationRequested request) {
        return ownerRepository.findByUserId(request.platformUserId())
                .map(owner -> owner.getId())
                .switchIfEmpty(Mono.defer(() -> createOwnerFacade.createForUser(
                        request.platformUserId(), toCreateRequest(request))
                        .map(PropertyOwnerResponse::id)))
                .flatMap(ownerId -> outboxWriter.appendGenerated(
                        RegistrationEventTopics.OWNER_PROVISIONED,
                        request.registrationId().toString(),
                        eventId -> objectMapper.writeValueAsString(new PropertyOwnerProvisioned(
                                eventId, request.registrationId(), request.platformUserId(),
                                ownerId, Instant.now(), request.businessType()))))
                .then();
    }

    private CreatePropertyOwnerRequest toCreateRequest(BusinessOwnerRegistrationRequested request) {
        return new CreatePropertyOwnerRequest(
                OwnerType.valueOf(request.ownerType()),
                request.displayName(),
                request.businessName(),
                request.businessRegistrationNumber(),
                request.taxNumber(),
                request.phoneNumber(),
                request.email(),
                request.telegram());
    }

    private void validate(BusinessOwnerRegistrationRequested request) {
        if (request == null || request.eventId() == null || request.registrationId() == null
                || request.platformUserId() == null || request.ownerType() == null
                || request.businessType() == null || request.businessType().isBlank()) {
            throw new IllegalArgumentException("Registration identifiers, owner type, and business type are required");
        }
    }
}
