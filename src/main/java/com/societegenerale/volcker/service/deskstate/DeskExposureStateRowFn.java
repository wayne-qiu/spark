package com.societegenerale.volcker.service.deskstate;

import com.societegenerale.volcker.entity.DeskExposure;
import com.societegenerale.volcker.entity.DeskState;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Iterator;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;

public class DeskExposureStateRowFn
    implements MapGroupsWithStateFunction<String, Row, DeskState, DeskExposure> {
    // key, value, 状态对象类型, 输出结果类型
    @Override
    public DeskExposure call(         // output type
        String deskId,                // key
        Iterator<Row> trades,         // value
        GroupState<DeskState> state   // state
    ) {

      DeskState current;
      if (state.exists() && !state.hasTimedOut()) {
        current = state.get();
      } else {
        current = new DeskState(); // Reset: 归零
        // 注意：如果这仅仅是一个 Timeout 事件（trades为空），
        // 下面的循环不会执行，current 保持为 0，最后输出就是 0。
      }

      LocalDateTime latestEventTime = current.getLastTradeTime();

      // row 类型
      while (trades.hasNext()) {
        Row t = trades.next();

        BigDecimal qty = t.getDecimal(t.fieldIndex("quantity"));
        BigDecimal notional = t.getDecimal(t.fieldIndex("notional"));
        String side = t.getString(t.fieldIndex("side"));
        LocalDateTime tradeTime = t.getTimestamp(t.fieldIndex("tradeTime")).toLocalDateTime();

        BigDecimal signedQty = "BUY".equalsIgnoreCase(side) ? qty : qty.negate();

        // cumulative
        current.setNetPosition(current.getNetPosition().add(signedQty));
        current.setTotalNotional(current.getTotalNotional().add(notional));
        current.setLastTradeTime(tradeTime);
        
        // Track the latest event time for timeout
        if (latestEventTime == null || tradeTime.isAfter(latestEventTime)) {
          latestEventTime = tradeTime;
        }
      }

      state.update(current); // update

      // Set Timeout: 10 mins after the latest event time
      // This requires .withWatermark() on the input stream
      // if watermark > Timeout, state.hasTimedOut flag up, do your action
      if (latestEventTime != null) {
        state.setTimeoutTimestamp(java.sql.Timestamp.valueOf(latestEventTime.plusMinutes(10)).getTime());
      }

      return new DeskExposure(
          deskId,
          current.getNetPosition(),
          current.getTotalNotional(),
          current.getLastTradeTime()
      );
    }
}
