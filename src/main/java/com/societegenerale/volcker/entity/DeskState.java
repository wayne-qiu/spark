package com.societegenerale.volcker.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class DeskState implements Serializable {

  private BigDecimal netPosition;
  private BigDecimal totalNotional;
  private LocalDateTime lastTradeTime;

  public DeskState() {
    this.netPosition = BigDecimal.ZERO;
    this.totalNotional = BigDecimal.ZERO;
  }
}