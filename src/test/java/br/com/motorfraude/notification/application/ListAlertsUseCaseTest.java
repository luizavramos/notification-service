package br.com.motorfraude.notification.application;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static br.com.motorfraude.notification.TestAlerts.id;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.usecase.AlertCursor;
import br.com.motorfraude.notification.application.usecase.AlertFilter;
import br.com.motorfraude.notification.application.usecase.InvalidQueryException;
import br.com.motorfraude.notification.application.usecase.ListAlertsUseCase;
import br.com.motorfraude.notification.fakes.InMemoryAlertQuery;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ListAlertsUseCaseTest {

    private final InMemoryAlertQuery query = new InMemoryAlertQuery();
    private final ListAlertsUseCase useCase = new ListAlertsUseCase(query);

    @BeforeEach
    void seed() {
        for (int i = 1; i <= 7; i++) {
            query.add(alert(id(i), "tx-listagem-%04d".formatted(i), i % 2 == 0 ? Severity.HIGH : Severity.MEDIUM, NOW.plusSeconds(i)));
        }
    }

    @Test
    @DisplayName("devolve o mais recente primeiro e um cursor só quando há mais")
    void primeiraPagina() {
        var page = useCase.list(AlertFilter.none(), null, 3);

        assertThat(page.items()).extracting(a -> a.alertId()).containsExactly(id(7), id(6), id(5));
        assertThat(page.nextCursor()).isNotNull();
    }

    @Test
    @DisplayName("seguir os cursores percorre tudo uma vez, sem repetir nem pular, e a última página não tem cursor")
    void percorreTudo() {
        var seen = new ArrayList<String>();
        String cursor = null;
        int pages = 0;
        do {
            var page = useCase.list(AlertFilter.none(), cursor, 3);
            page.items().forEach(a -> seen.add(a.alertId()));
            cursor = page.nextCursor();
            pages++;
        } while (cursor != null);

        assertThat(seen).hasSize(7).doesNotHaveDuplicates();
        assertThat(pages).isEqualTo(3);
    }

    @Test
    @DisplayName("página cheia exata: 6 alertas com limite 3 dá 2 páginas e a segunda já vem sem cursor")
    void paginaCheiaExata() {
        var q = new InMemoryAlertQuery();
        for (int i = 1; i <= 6; i++) {
            q.add(alert(id(i), "tx-exata-%04d".formatted(i), Severity.HIGH, NOW.plusSeconds(i)));
        }
        var uc = new ListAlertsUseCase(q);

        var second = uc.list(AlertFilter.none(), uc.list(AlertFilter.none(), null, 3).nextCursor(), 3);

        assertThat(second.items()).hasSize(3);
        assertThat(second.nextCursor()).isNull();
    }

    @Test
    @DisplayName("alertas com o mesmo detectedAt não se perdem entre páginas (desempate por alertId)")
    void empate() {
        var q = new InMemoryAlertQuery();
        for (int i = 1; i <= 5; i++) {
            q.add(alert(id(i), "tx-empate-%04d".formatted(i), Severity.HIGH, NOW));
        }
        var uc = new ListAlertsUseCase(q);
        var seen = new HashSet<String>();
        String cursor = null;
        do {
            var page = uc.list(AlertFilter.none(), cursor, 2);
            page.items().forEach(a -> seen.add(a.alertId()));
            cursor = page.nextCursor();
        } while (cursor != null);

        assertThat(seen).hasSize(5);
    }

    @Test
    @DisplayName("filtra por conta, severidade e a partir de um instante")
    void filtros() {
        assertThat(useCase.list(new AlertFilter(ACCOUNT, Severity.HIGH, null), null, 50).items()).hasSize(3);
        assertThat(useCase.list(new AlertFilter("outra-conta-00000001", null, null), null, 50).items()).isEmpty();
        assertThat(useCase.list(new AlertFilter(null, null, NOW.plusSeconds(5)), null, 50).items()).hasSize(3);
    }

    @Test
    @DisplayName("sem limite usa 50, e o limite fora de 1 a 200 é consulta inválida")
    void limite() {
        assertThat(useCase.list(AlertFilter.none(), null, null).items()).hasSize(7);
        assertThatThrownBy(() -> useCase.list(AlertFilter.none(), null, 0)).isInstanceOf(InvalidQueryException.class);
        assertThatThrownBy(() -> useCase.list(AlertFilter.none(), null, 201)).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    @DisplayName("cursor adulterado vira consulta inválida, e não erro interno")
    void cursorInvalido() {
        assertThatThrownBy(() -> useCase.list(AlertFilter.none(), "isto-nao-e-um-cursor", 10)).isInstanceOf(InvalidQueryException.class);
        assertThatThrownBy(() -> AlertCursor.decode("")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    @DisplayName("o cursor codifica e decodifica o mesmo ponto, com precisão de microssegundo")
    void cursorIdaEVolta() {
        var cursor = new AlertCursor(Instant.parse("2026-11-15T12:00:00.123456Z"), id(3));

        assertThat(AlertCursor.decode(cursor.encode())).isEqualTo(cursor);
    }
}
