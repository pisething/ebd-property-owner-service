package com.pisethjavaschool.propertyowner.facade;

import com.pisethjavaschool.propertyowner.dto.CreatePropertyOwnerRequest;
import com.pisethjavaschool.propertyowner.dto.PropertyOwnerResponse;

import reactor.core.publisher.Mono;

public interface CreatePropertyOwnerFacade {
    Mono<PropertyOwnerResponse> create(CreatePropertyOwnerRequest request);
}
