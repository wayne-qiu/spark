package com.societegenerale.volcker.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RTrade {
  private String tradeId;
  private LocalDateTime tradeTime;
  private String traderId;       // 或 deskId
  private String instrumentId;
  private String instrumentType;
  private String cusip;
  private String side;           // "BUY" 或 "SELL"
  private BigDecimal quantity;
  private BigDecimal price;
  private BigDecimal notional;
}
