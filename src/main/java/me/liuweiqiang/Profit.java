package me.liuweiqiang;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.BigDecSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class Profit {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000 * 60 * 30); // onPeriodicEmit周期
        // 用文件模拟事件
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(Objects.requireNonNull(Profit.class.getClassLoader().getResource("records"))
                                .toURI()))
                .build();
        WatermarkStrategy<String> strategy = new WatermarkStrategy<String>() {
            @Override // 使用递增的逻辑时间
            public TimestampAssigner<String> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new TimestampAssigner<String>() {
                    private final AtomicLong aLong = new AtomicLong(Long.MIN_VALUE);
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return aLong.getAndIncrement();
                    }
                };
            }
            @Override
            public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<String>() {
                    @Override
                    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
                        Record r;
                        if ((r = Record.deserialize(event)) != null && r instanceof Nav) {
                            output.emitWatermark(new Watermark(eventTimestamp));
                        }
                    }
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {}
                };
            }
        }.withIdleness(Duration.ofMinutes(30));
        DataStreamSource<String> source = env
                .fromSource(fileSource, strategy, "profit");
        // 验证相同source的两个不同实例有不同的时间
        DataStreamSource<String> source2 = env
                .fromSource(fileSource, strategy, "profit2");
        ProcessFunction<Object, String> withTime = new ProcessFunction<Object, String>() {
            @Override
            public void processElement(Object value, ProcessFunction<Object, String>.Context ctx, Collector<String> out) {
                out.collect(ctx.timestamp() + ":" + value);
            }
        };
        source.map(str -> (Object) str).process(withTime).print();
        source2.map(str -> (Object) str).process(withTime).print();
        DataStream<Record> mapped = source
                .flatMap(new FlatMapFunction<String, Record>() {
                    @Override
                    public void flatMap(String value, Collector<Record> out) {
                        Record record = Record.deserialize(value);
                        if (record != null) {
                            out.collect(record);
                        }
                    }
                });
        mapped.keyBy(value -> 1).flatMap(new Idempotency()).keyBy(value -> 1)
                .flatMap(new MainProcessor()).map(decimal -> (Object) decimal).process(withTime).print();
        env.execute();
    }

    // 实现应用层幂等，即使source支持exactly-once（Flink应用作为消费者），但在生产端可能会重复发送
    public static class Idempotency extends RichFlatMapFunction<Record, Record> {
        private MapState<String, Boolean> processed;
        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Boolean> mapStateDescriptor = new MapStateDescriptor<>("processed", String.class, Boolean.class);
            processed = this.getRuntimeContext().getMapState(mapStateDescriptor);
            StateTtlConfig stateTtlConfig = StateTtlConfig
                    .newBuilder(Time.days(7))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            mapStateDescriptor.enableTimeToLive(stateTtlConfig);
        }
        @Override
        public void flatMap(Record value, Collector<Record> out) throws Exception {
            if (value instanceof Transaction) {
                Transaction transaction = (Transaction) value;
                if (processed.get(transaction.no) != null) {
                    return;
                }
                processed.put(transaction.no, true);
            }
            out.collect(value);
        }
    }

    // 计算盈亏
    // 盈亏 = 浮动盈亏（净值浮动） + 实现盈亏（交易）
    // 模拟数据中赎回不含手续费，不含分红数据
    public static class MainProcessor extends RichFlatMapFunction<Record, BigDecimal> {
        private ValueState<BigDecimal> share;
        private ValueState<BigDecimal> nav;
        private ValueState<String> navDate;
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<BigDecimal> valueStateDescriptor = new ValueStateDescriptor<>("share", new BigDecSerializer());
            share = this.getRuntimeContext().getState(valueStateDescriptor);
            ValueStateDescriptor<BigDecimal> navDescriptor = new ValueStateDescriptor<>("nav", new BigDecSerializer());
            nav = this.getRuntimeContext().getState(navDescriptor);
            ValueStateDescriptor<String> navDateDescriptor = new ValueStateDescriptor<>("navDate", String.class);
            navDate = this.getRuntimeContext().getState(navDateDescriptor);
        }
        @Override
        public void flatMap(Record value, Collector<BigDecimal> out) throws IOException {
            BigDecimal lastShare = share.value() == null? BigDecimal.ZERO: share.value();
            BigDecimal lastNav = nav.value() == null? BigDecimal.ZERO: nav.value();
            if (value instanceof Transaction) {
                Transaction transaction;
                BigDecimal profit = (transaction = (Transaction) value).doProfit(lastNav);
                out.collect(profit);
                BigDecimal newShare = transaction.maintainShare(lastShare);
                share.update(newShare);
                return;
            }
            Nav newNav = (Nav) value;
            if (navDate.value() != null && newNav.navDate.compareTo(navDate.value()) < 0) {
                return;
            }
            BigDecimal profit = newNav.doProfit(lastNav, lastShare);
            out.collect(profit);
            navDate.update(newNav.navDate);
            nav.update(newNav.nav);
        }
    }

    public static class Record {

        public static Record deserialize(String str) {
            String[] parts = StringUtils.split(str, ',');
            if (parts.length == 4 && "3".equals(parts[0])) {
                RedTransaction redTransaction = new RedTransaction();
                redTransaction.amount = new BigDecimal(parts[1]);
                redTransaction.share = new BigDecimal(parts[2]);
                redTransaction.no = parts[3];
                return redTransaction;
            } else if (parts.length == 4 && "2".equals(parts[0])) {
                SubTransaction subTransaction = new SubTransaction();
                subTransaction.amount = new BigDecimal(parts[1]);
                subTransaction.share = new BigDecimal(parts[2]);
                subTransaction.no = parts[3];
                return subTransaction;
            } else if (parts.length == 3 && "1".equals(parts[0])) {
                Nav nav = new Nav();
                nav.nav = new BigDecimal(parts[1]);
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                try {
                    simpleDateFormat.parse(parts[2]);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                nav.navDate = parts[2];
                return nav;
            }
            return null;
        }

    }

    public static class Nav extends Record {
        private BigDecimal nav;
        private String navDate;
        @Override
        public String toString() {
            return "Nav{" +
                    "nav=" + nav.toPlainString() +
                    ", navDate='" + navDate + '\'' +
                    '}';
        }
        public BigDecimal doProfit(BigDecimal lastNav, BigDecimal share) {
            return nav.subtract(lastNav).multiply(share);
        }
    }

    public static abstract class Transaction extends Record {
        String no;
        BigDecimal amount;
        BigDecimal share;
        @Override
        public String toString() {
            return "Transaction{" +
                    "no='" + no + '\'' +
                    ", amount=" + amount +
                    ", share=" + share +
                    '}';
        }
        public abstract BigDecimal doProfit(BigDecimal lastNav);
        public abstract BigDecimal maintainShare(BigDecimal old);
    }
    
    public static class SubTransaction extends Transaction {
        @Override
        public BigDecimal doProfit(BigDecimal lastNav) {
            return share.multiply(lastNav).subtract(amount);
        }
        @Override
        public BigDecimal maintainShare(BigDecimal old) {
            return share.add(old);
        }
    }

    public static class RedTransaction extends Transaction {
        @Override
        public BigDecimal doProfit(BigDecimal lastNav) {
            return amount.subtract(share.multiply(lastNav));
        }
        @Override
        public BigDecimal maintainShare(BigDecimal old) {
            return old.subtract(share);
        }
    }

}
