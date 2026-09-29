package com.aia.voiceinsights.api.model;

public record ResetPasswordRequest(String email, String newPassword, String confirmPassword) {}
