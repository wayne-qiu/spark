package com.societegenerale.volcker.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Trade implements Serializable {

  // --- 原 Trade 字段 ---
  private String tradeId;
  private LocalDateTime tradeTime;
  private String traderId;
  private String instrumentId;
  private String instrumentType;
  private String cusip;
  private String side;
  private BigDecimal quantity;
  private BigDecimal price;
  private BigDecimal notional;

  // --- Enrichment 字段 ---
  private String deskId;

}
