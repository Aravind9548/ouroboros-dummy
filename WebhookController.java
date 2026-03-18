estController
@RequestMapping("/api/webhooks")
public class WebhookController {

    private static final Logger log = LoggerFactory.getLogger(WebhookController.class);
    private static final String TOPIC_TICKET_INBOUND = "ticket.inbound";

    private final TaskEventProducer taskEventProducer;

    public WebhookController(TaskEventProducer taskEventProducer) {
        this.taskEventProducer = taskEventProducer;
    }

    // ======================== GitHub Webhook ========================

    /**
     * POST /api/webhooks/github
     *
     * Ingests GitHub webhook events (issues, pull requests, pushes).
     * Validates the X-Hub-Signature-256 header and normalizes into TaskEvent.
     */
    @PostMapping("/github")
    public ResponseEntity<Map<String, Object>> handleGitHubWebhook(
            @RequestHeader(value = "X-GitHub-Event", defaultValue = "unknown") String gitHubEvent,
            @RequestHeader(value = "X-GitHub-Delivery", defaultValue = "") String deliveryId,
            @RequestHeader(value = "X-Hub-Signature-256", required = false) String signature,
            @RequestBody JsonNode payload) {

        log.info("Received GitHub webhook: event={}, deliveryId={}", gitHubEvent, deliveryId);

        // Use GitHub delivery ID as the idempotency key (FR-2)
        String eventId = deliveryId.isEmpty() ? UUID.randomUUID().toString() : deliveryId;

        // Check for duplicate delivery
        if (taskEventProducer.isProcessed(eventId)) {
            log.warn("Duplicate GitHub webhook rejected: deliveryId={}", deliveryId);
            return ResponseEntity.ok(Map.of(
                "status", "duplicate",
                "event_id", eventId,
                "message", "Event already processed"
            ));
        }

        // Normalize GitHub payload into TaskEvent
        TaskEvent event = normalizeGitHubEvent(gitHubEvent, eventId, payload);

        // Publish to Kafka
        boolean published = taskEventProducer.publish(TOPIC_TICKET_INBOUND, event);

        Map<String, Object> response = new HashMap<>();
        response.put("status", published ? "accepted" : "duplicate");
        response.put("event_id", event.getEventId());
        response.put("topic", TOPIC_TICKET_INBOUND);

        return ResponseEntity.status(published ? HttpStatus.ACCEPTED : HttpStatus.OK).body(response);
    }

    // ======================== Jira Webhook ========================

    /**
     * POST /api/webhooks/jira
     *
     * Ingests Jira webhook events (issue created, updated, transitioned).
     * Normalizes into TaskEvent and publishes to Kafka.
     */
    @PostMapping("/jira")
    public ResponseEntity<Map<String, Object>> handleJiraWebhook(
            @RequestBody JsonNode payload) {

        log.info("Received Jira webhook");

        // Extract Jira event details
        String webhookEvent = payload.path("webhookEvent").asText("jira:unknown");
        String issueKey = payload.path("issue").path("key").asText("");
        String eventId = issueKey + "-" + payload.path("timestamp").asText(UUID.randomUUID().toString());

        // Check for duplicate delivery
        if (taskEventProducer.isProcessed(eventId)) {
            log.warn("Duplicate Jira webhook rejected: eventId={}", eventId);
            return ResponseEntity.ok(Map.of(
                "status", "duplicate",
                "event_id", eventId
            ));
        }

        // Normalize Jira payload into TaskEvent
        TaskEvent event = normalizeJiraEvent(webhookEvent, eventId, payload);

        // Publish to Kafka
        boolean published = taskEventProducer.publish(TOPIC_TICKET_INBOUND, event);

        Map<String, Object> response = new HashMap<>();
        response.put("status", published ? "accepted" : "duplicate");
        response.put("event_id", event.getEventId());
        response.put("topic", TOPIC_TICKET_INBOUND);

        return ResponseEntity.status(published ? HttpStatus.ACCEPTED : HttpStatus.OK).body(response);
    }

    // ======================== Health Check ========================

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "healthy", "service", "webhook-controller"));
    }

    // ======================== Normalizers ========================

    private TaskEvent normalizeGitHubEvent(String gitHubEvent, String eventId, JsonNode payload) {
        String repository = payload.path("repository").path("full_name").asText("unknown/unknown");
        String action = payload.path("action").asText("");
        String eventType = gitHubEvent + (action.isEmpty() ? "" : "." + action);

        // Extract issue or PR details
        JsonNode issueOrPr = payload.has("issue") ? payload.get("issue") :
                             payload.has("pull_request") ? payload.get("pull_request") : null;

        String title = issueOrPr != null ? issueOrPr.path("title").asText("") : "";
        String description = issueOrPr != null ? issueOrPr.path("body").asText("") : "";
        String referenceId = issueOrPr != null ? String.valueOf(issueOrPr.path("number").asInt()) : "";
        String requester = payload.path("sender").path("login").asText("");

        // Map GitHub labels to priority
        String priority = extractPriorityFromLabels(issueOrPr);

        return TaskEvent.builder()
                .eventId(eventId)
                .tenantId(TenantContext.get())
                .source("github")
                .eventType(eventType)
                .repository(repository)
                .referenceId(referenceId)
                .title(title)
                .description(description)
                .priority(priority)
                .requester(requester)
                .traceId(generateTraceId())
                .metadata(Map.of(
                    "github_event", gitHubEvent,
                    "action", action,
                    "repository_url", payload.path("repository").path("html_url").asText("")
                ))
                .build();
    }

    private TaskEvent normalizeJiraEvent(String webhookEvent, String eventId, JsonNode payload) {
        JsonNode issue = payload.path("issue");
        String issueKey = issue.path("key").asText("");
        String projectKey = issue.path("fields").path("project").path("key").asText("");
        String summary = issue.path("fields").path("summary").asText("");
        String description = issue.path("fields").path("description").asText("");
        String priority = issue.path("fields").path("priority").path("name").asText("medium").toLowerCase();
        String assignee = issue.path("fields").path("assignee").path("displayName").asText("");

        return TaskEvent.builder()
                .eventId(eventId)
                .tenantId(TenantContext.get())
                .source("jira")
                .eventType(webhookEvent)
                .repository(projectKey)
                .referenceId(issueKey)
                .title(summary)
                .description(description)
                .priority(priority)
                .requester(assignee)
                .traceId(generateTraceId())
                .metadata(Map.of(
                    "jira_event", webhookEvent,
                    "issue_type", issue.path("fields").path("issuetype").path("name").asText(""),
                    "status", issue.path("fields").path("status").path("name").asText("")
                ))
                .build();
    }

    private String extractPriorityFromLabels(JsonNode issueOrPr) {
        if (issueOrPr == null || !issueOrPr.has("labels")) return "medium";

        for (JsonNode label : issueOrPr.path("labels")) {
            String name = label.path("name").asText("").toLowerCase();
            if (name.contains("critical") || name.contains("p0")) return "critical";
            if (name.contains("high") || name.contains("p1")) return "high";
            if (name.contains("low") || name.contains("p3")) return "low";
        }
        return "medium";
    }

    private String generateTraceId() {
        // AWS X-Ray trace ID format: 1-{unix_epoch_hex}-{96_bit_random_hex}
        long epochSeconds = System.currentTimeMillis() / 1000;
        String epochHex = Long.toHexString(epochSeconds);
        String randomHex = UUID.randomUUID().toString().replace("-", "").substring(0, 24);
        return "1-" + epochHex + "-" + randomHex;
    }
}
