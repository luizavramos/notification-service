package br.com.motorfraude.notification.adapters.controller.rest;

import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.usecase.AlertFilter;
import br.com.motorfraude.notification.application.usecase.AlertPage;
import br.com.motorfraude.notification.application.usecase.InvalidQueryException;
import br.com.motorfraude.notification.application.usecase.ListAlertsUseCase;
import br.com.motorfraude.notification.application.usecase.RegisterFeedbackUseCase;
import br.com.motorfraude.notification.application.usecase.Verdict;
import java.time.Instant;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/alerts")
public class AlertController {

    public record FeedbackRequest(Verdict verdict) {
    }

    private final ListAlertsUseCase list;
    private final RegisterFeedbackUseCase feedback;

    public AlertController(ListAlertsUseCase list, RegisterFeedbackUseCase feedback) {
        this.list = list;
        this.feedback = feedback;
    }

    @GetMapping
    public AlertPage list(@RequestParam(required = false) String accountId, @RequestParam(required = false) Severity severity,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) String cursor, @RequestParam(required = false) Integer limit) {
        return list.list(new AlertFilter(accountId, severity, from), cursor, limit);
    }

    @PostMapping("/{alertId}/feedback")
    @ResponseStatus(HttpStatus.CREATED)
    public void feedback(@PathVariable String alertId, @RequestBody FeedbackRequest request,
            @RequestHeader(name = "X-Analyst", required = false) String analyst) {
        if (request.verdict() == null) {
            throw new InvalidQueryException("verdict é obrigatório: CONFIRMED_FRAUD ou FALSE_POSITIVE");
        }
        feedback.register(alertId, request.verdict(), analyst);
    }
}
