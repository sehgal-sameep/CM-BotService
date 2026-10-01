package com.cmbotservice.web.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ChatRequestTest {

  @Test
  void blankOptionalIdentifiers_areNormalizedToNull() {
    ChatRequest request = new ChatRequest(" ", null, null, "", "  ", "hello");

    assertThat(request.caseId()).isNull();
    assertThat(request.endUserId()).isNull();
    assertThat(request.operatorId()).isNull();
  }

  @Test
  void suppliedIdentifiers_areKeptAsIs_andNeverSwapped() {
    ChatRequest request = new ChatRequest("case-1", null, null, "end-user-9", "op-7", "hello");

    assertThat(request.endUserId()).isEqualTo("end-user-9");
    assertThat(request.operatorId()).isEqualTo("op-7");
  }
}
