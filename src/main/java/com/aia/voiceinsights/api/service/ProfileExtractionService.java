package com.aia.voiceinsights.api.service;

import com.aia.voiceinsights.api.model.CustomerProfile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.ResponseFormat;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Turns the running voice-session transcript into structured {@link
 * CustomerProfile} fields — called by VoiceWebSocketHandler after each final
 * transcript segment (and once more at session end). Runs against the FULL
 * transcript so far each time (not incrementally), so it's safe to just
 * overwrite prior field values with each call's output.
 */
@Service
public class ProfileExtractionService {

    private static final ResponseFormat JSON_FORMAT =
            ResponseFormat.builder().type(ResponseFormat.Type.JSON_OBJECT).build();

    private static final String SYSTEM_PROMPT = """
            You are extracting a structured customer profile from an insurance
            agent's live conversation with a customer, transcribed so far. Extract
            ONLY facts actually stated or clearly implied in the transcript; leave
            a field null (or an empty array) if it hasn't come up yet. Never invent
            details that were not said.

            Respond with ONLY a JSON object, no markdown fences, no commentary:
            {
              "customerName": string or null,
              "age": integer or null,
              "occupation": string or null,
              "incomeBand": string or null,
              "dependents": integer or null,
              "existingPolicies": [string],
              "goalsAndConcerns": [string],
              "budgetNotes": string or null,
              "notes": "any other relevant free-text context, or null"
            }
            """;

    private final ChatModel chatModel;
    private final ObjectMapper mapper = new ObjectMapper();

    public ProfileExtractionService(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    /** Best-effort: on any failure, the profile is left with whatever it already had. */
    public void extractInto(CustomerProfile profile, String transcript) {
        if (transcript == null || transcript.isBlank()) return;

        try {
            var response = chatModel.call(new Prompt(
                    List.of(new SystemMessage(SYSTEM_PROMPT), new UserMessage("Transcript so far:\n" + transcript)),
                    OpenAiChatOptions.builder().responseFormat(JSON_FORMAT).build()));

            JsonNode root = mapper.readTree(response.getResult().getOutput().getText());

            applyIfPresent(root, "customerName", profile::setCustomerName);
            if (root.hasNonNull("age")) profile.setAge(root.get("age").asInt());
            applyIfPresent(root, "occupation", profile::setOccupation);
            applyIfPresent(root, "incomeBand", profile::setIncomeBand);
            if (root.hasNonNull("dependents")) profile.setDependents(root.get("dependents").asInt());
            profile.setExistingPolicies(toList(root.path("existingPolicies")));
            profile.setGoalsAndConcerns(toList(root.path("goalsAndConcerns")));
            applyIfPresent(root, "budgetNotes", profile::setBudgetNotes);
            applyIfPresent(root, "notes", profile::setNotes);
        } catch (Exception e) {
            // Best-effort — leave the profile's prior fields untouched, but log it:
            // a silent catch here previously hid a real bug with no trace at all.
            System.err.println("ProfileExtractionService: extraction failed: " + e);
        }
    }

    private void applyIfPresent(JsonNode root, String field, Consumer<String> setter) {
        if (root.hasNonNull(field)) setter.accept(root.get(field).asText());
    }

    private List<String> toList(JsonNode arr) {
        List<String> out = new ArrayList<>();
        if (arr.isArray()) for (JsonNode n : arr) out.add(n.asText());
        return out;
    }
}
