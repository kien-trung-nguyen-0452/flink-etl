package vn.softdreams.flink.repository;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//add comment
@Slf4j
public class ClickHouseSink extends RichSinkFunction<RepositoryLedgerRecord> {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int    port;
    private final String database;
    private final String user;
    private final String password;
    private final String table;
    private final int    batchSize;
    private final long   flushIntervalMs;

    private transient Connection        conn;
    private transient PreparedStatement stmt;
    private transient List<RepositoryLedgerRecord> buffer;
    private transient long lastFlushTime;

    private static final String INSERT_SQL =
            "INSERT INTO %s (" +
                    "ID,CompanyID,BranchID,ReferenceID,Date,PostedDate,TypeLedger," +
                    "NoFBook,NoMBook,Account,AccountCorresponding," +
                    "RepositoryID,RepositoryCode,RepositoryName," +
                    "MaterialGoodsID,MaterialGoodsCode,MaterialGoodsName," +
                    "UnitID,UnitPrice,IWQuantity,OWQuantity,IWAmount,OWAmount," +
                    "MainUnitID,MainUnitPrice,MainIWQuantity,MainOWQuantity,MainConvertRate,Formula," +
                    "Reason,Description,ExpiryDate,LotNo," +
                    "BudgetItemID,CostSetID,StatisticsCodeID,ExpenseItemID," +
                    "DetailID,TypeID,OrderPriority,ConfrontID,ConfrontDetailID," +
                    "IsPromotion,RefDateTime,DepartmentID,AccountingObjectID,ContractID," +
                    "CustomField1,CustomField2,CustomField3,CustomField4,CustomField5," +
                    "CustomFieldDetail1,CustomFieldDetail2,CustomFieldDetail3,CustomFieldDetail4,CustomFieldDetail5," +
                    "created_date,RefID," +
                    "__source_ts_ms,__deleted,cluster_id" +
                    ") VALUES (" +
                    "?,?,?,?,?,?,?," +   // ID..TypeLedger
                    "?,?,?,?," +          // NoFBook..AccountCorresponding
                    "?,?,?," +            // RepositoryID..RepositoryName
                    "?,?,?," +            // MaterialGoodsID..MaterialGoodsName
                    "?,?,?,?,?,?," +      // UnitID..OWAmount
                    "?,?,?,?,?,?," +      // MainUnitID..Formula
                    "?,?,?,?," +          // Reason..LotNo
                    "?,?,?,?," +          // BudgetItemID..ExpenseItemID
                    "?,?,?,?,?," +        // DetailID..ConfrontDetailID
                    "?,?,?,?,?," +        // IsPromotion..ContractID
                    "?,?,?,?,?," +        // CustomField1..5
                    "?,?,?,?,?," +        // CustomFieldDetail1..5
                    "?,?," +              // created_date,RefID
                    "?,?,?" +             // __source_ts_ms,__deleted,cluster_id
                    ")";

    public ClickHouseSink(String host, int port, String database,
                          String user, String password, String table,
                          int batchSize, long flushIntervalMs) {
        this.host            = host;
        this.port            = port;
        this.database        = database;
        this.user            = user;
        this.password        = password;
        this.table           = table;
        this.batchSize       = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        conn          = buildConnection();
        stmt          = conn.prepareStatement(String.format(INSERT_SQL, table));
        buffer        = new ArrayList<>(batchSize);
        lastFlushTime = System.currentTimeMillis();
        log.info("ClickHouseSink opened | {}:{} table={} batch={} flush={}ms",
                host, port, table, batchSize, flushIntervalMs);
    }

    @Override
    public void invoke(RepositoryLedgerRecord r, Context ctx) throws Exception {
        if (r == null) return;
        buffer.add(r);
        long now = System.currentTimeMillis();
        if (buffer.size() >= batchSize || (now - lastFlushTime) >= flushIntervalMs) {
            flush();
        }
    }

    private void flush() throws Exception {
        if (buffer.isEmpty()) return;
        int count = 0;
        try {
            for (RepositoryLedgerRecord r : buffer) {
                bind(stmt, r);
                stmt.addBatch();
                count++;
            }
            stmt.executeBatch();
            conn.commit();
            lastFlushTime = System.currentTimeMillis();
            log.debug("Flushed {} records", count);
        } catch (Exception e) {
            log.error("Flush failed ({} records): {}", count, e.getMessage(), e);
            try { conn.rollback(); } catch (Exception ignored) {}
            reconnect();
            throw e;
        } finally {
            buffer.clear();
            stmt.clearBatch();
        }
    }

    private void bind(PreparedStatement s, RepositoryLedgerRecord r) throws Exception {
        int i = 1;
        s.setString(i++, r.getId());
        s.setString(i++, r.getCompanyId());
        s.setString(i++, r.getBranchId());
        s.setString(i++, r.getReferenceId());
        ts(s, i++, r.getDate());           // DateTime ← epoch ms
        ts(s, i++, r.getPostedDate());     // DateTime ← epoch ms
        nullInt(s, i++, r.getTypeLedger());
        s.setString(i++, r.getNoFBook());
        s.setString(i++, r.getNoMBook());
        s.setString(i++, r.getAccount());
        s.setString(i++, r.getAccountCorresponding());
        s.setString(i++, r.getRepositoryId());
        s.setString(i++, r.getRepositoryCode());
        s.setString(i++, r.getRepositoryName());
        s.setString(i++, r.getMaterialGoodsId());
        s.setString(i++, r.getMaterialGoodsCode());
        s.setString(i++, r.getMaterialGoodsName());
        s.setString(i++, r.getUnitId());
        nullDec(s, i++, r.getUnitPrice());
        nullDec(s, i++, r.getIwQuantity());
        nullDec(s, i++, r.getOwQuantity());
        nullDec(s, i++, r.getIwAmount());
        nullDec(s, i++, r.getOwAmount());
        s.setString(i++, r.getMainUnitId());
        nullDec(s, i++, r.getMainUnitPrice());
        nullDec(s, i++, r.getMainIwQuantity());
        nullDec(s, i++, r.getMainOwQuantity());
        nullDec(s, i++, r.getMainConvertRate());
        s.setString(i++, r.getFormula());
        s.setString(i++, r.getReason());
        s.setString(i++, r.getDescription());
        ts(s, i++, r.getExpiryDate());     // DateTime ← epoch ms
        s.setString(i++, r.getLotNo());
        s.setString(i++, r.getBudgetItemId());
        s.setString(i++, r.getCostSetId());
        s.setString(i++, r.getStatisticsCodeId());
        s.setString(i++, r.getExpenseItemId());
        s.setString(i++, r.getDetailId());
        nullInt(s, i++, r.getTypeId());
        nullInt(s, i++, r.getOrderPriority());
        s.setString(i++, r.getConfrontId());
        s.setString(i++, r.getConfrontDetailId());
        nullInt(s, i++, r.getIsPromotion());
        ts(s, i++, r.getRefDateTime());    // DateTime ← epoch ms
        s.setString(i++, r.getDepartmentId());
        s.setString(i++, r.getAccountingObjectId());
        s.setString(i++, r.getContractId());
        s.setString(i++, r.getCustomField1());
        s.setString(i++, r.getCustomField2());
        s.setString(i++, r.getCustomField3());
        s.setString(i++, r.getCustomField4());
        s.setString(i++, r.getCustomField5());
        s.setString(i++, r.getCustomFieldDetail1());
        s.setString(i++, r.getCustomFieldDetail2());
        s.setString(i++, r.getCustomFieldDetail3());
        s.setString(i++, r.getCustomFieldDetail4());
        s.setString(i++, r.getCustomFieldDetail5());
        ts(s, i++, r.getCreatedDate());    // DateTime ← epoch ms
        s.setString(i++, r.getRefId());
        s.setLong(i++, r.getSourceTs());
        s.setInt(i++, r.getDeleted());
        s.setString(i++, r.getClusterId());
    }

    // ── Type helpers ─────────────────────────────────────────────────────────

    /** Debezium epoch ms → java.sql.Timestamp → CH DateTime */
    private static void ts(PreparedStatement s, int idx, Long epochMs) throws SQLException {
        if (epochMs == null) s.setNull(idx, Types.TIMESTAMP);
        else s.setTimestamp(idx, new Timestamp(epochMs));
    }

    private static void nullInt(PreparedStatement s, int idx, Integer val) throws SQLException {
        if (val == null) s.setNull(idx, Types.INTEGER);
        else s.setInt(idx, val);
    }

    private static void nullDec(PreparedStatement s, int idx, BigDecimal val) throws SQLException {
        if (val == null) s.setNull(idx, Types.DECIMAL);
        else s.setBigDecimal(idx, val);
    }

    // ── Connection management ─────────────────────────────────────────────────

    private Connection buildConnection() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("socket_timeout", "600000");
        props.setProperty("connect_timeout", "30000");
        props.setProperty("compress", "1");
        String url = String.format(
                "jdbc:clickhouse://%s:%d/%s" +
                        "?compress=1" +
                        "&socket_timeout=600000" +
                        "&connect_timeout=30000" +
                        "&max_execution_time=600" +
                        "&buffer_size=1048576",   // 1MB buffer
                host, port, database
        );
        Connection c = java.sql.DriverManager.getConnection(url, props);
        c.setAutoCommit(false);
        return c;
    }

    private void reconnect() {
        log.warn("Reconnecting to ClickHouse...");
        try { if (conn != null && !conn.isClosed()) conn.close(); } catch (Exception ignored) {}
        try {
            conn  = buildConnection();
            stmt  = conn.prepareStatement(String.format(INSERT_SQL, table));
            log.info("Reconnected to ClickHouse");
        } catch (Exception e) {
            log.error("Reconnect failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        try { flush(); } catch (Exception e) { log.error("Final flush error", e); }
        if (stmt != null) try { stmt.close(); } catch (Exception ignored) {}
        if (conn  != null) try { conn.close();  } catch (Exception ignored) {}
    }
}