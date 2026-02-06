package com.societegenerale.volcker.service.deskstate;

import com.societegenerale.volcker.entity.DeskExposure;
import com.societegenerale.volcker.entity.DeskState;
import com.societegenerale.volcker.entity.Trade;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Iterator;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

public class DeskExposureStateObjectFn
    implements MapGroupsWithStateFunction<String, Trade, DeskState, DeskExposure> {
    // key, value, 状态对象类型, 输出结果类型
    @Override
    public DeskExposure call(         // output type
        String deskId,                // key
        Iterator<Trade> trades,         // value
        GroupState<DeskState> state   // state
    ) {

      DeskState current = state.exists() ? state.get() : new DeskState();

      // row 类型
      while (trades.hasNext()) {
        Trade t = trades.next();

        BigDecimal qty = t.getQuantity();
        BigDecimal notional = t.getNotional();
        String side = t.getSide();
        LocalDateTime tradeTime = t.getTradeTime();

        BigDecimal signedQty = "BUY".equalsIgnoreCase(side) ? qty : qty.negate();

        // cumulative
        current.setNetPosition(current.getNetPosition().add(signedQty));
        current.setTotalNotional(current.getTotalNotional().add(notional));
        current.setLastTradeTime(tradeTime);
      }

      state.update(current); // update

      return new DeskExposure(
          deskId,
          current.getNetPosition(),
          current.getTotalNotional(),
          current.getLastTradeTime()
      );
    }
}
