package com.cmbotservice.security;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Logs a loud, impossible-to-miss warning on every startup while authentication is bypassed ({@code
 * chatbot.security.mode: NONE}) — exactly as the documented flow requires ("must log a loud warning
 * on startup"). Local development only; never expected to be active in a shared/prod environment.
 */
@Component
@ConditionalOnProperty(
    prefix = "chatbot.security",
    name = "mode",
    havingValue = "NONE",
    matchIfMissing = true)
@Slf4j
public class SecurityModeStartupLogger {

  @PostConstruct
  void warnAuthenticationBypassed() {
    log.warn(
        """

                ##########################################################################
                # chatbot.security.mode = NONE                                           #
                # ALL AUTHENTICATION IS BYPASSED. Every request is permitted unchanged.   #
                # This is a local-development-only setting.                              #
                # NEVER run with this mode in a shared or production environment.         #
                ##########################################################################
                """);
  }
}
