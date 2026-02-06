package com.societegenerale.volcker.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeskExposure {

  private String deskId;
  private BigDecimal netPosition;
  private BigDecimal totalNotional;
  private LocalDateTime asOf;

}
