package com.tokendashboard.model;

import java.time.Instant;

public record UsageEvent(String clientId, int tokens, Instant timestamp) {
}
