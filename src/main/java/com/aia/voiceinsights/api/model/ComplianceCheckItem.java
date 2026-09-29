package com.aia.voiceinsights.api.model;

import java.io.Serializable;

public record ComplianceCheckItem(String check, boolean passed, String note) implements Serializable {}
