# notification-service

Entrega os alertas do motor (`fraud-alerts.v1`): ao time antifraude por webhook assinado (todo alerta) e ao cliente por push, e-mail e SMS de reserva (só HIGH), sem duplicar e sem travar um canal por causa de outro. Spring Boot 4, virtual threads, porta 8085.

Build: `./mvnw verify`. Camadas: `domain` ← `application` ← `adapters` ← `infrastructure` (ArchUnit).
