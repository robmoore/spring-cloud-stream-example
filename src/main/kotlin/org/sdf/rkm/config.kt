package org.sdf.rkm

import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.context.annotation.Configuration

@Configuration
@EnableBinding(Processor::class)
class IntegrationConfiguration
