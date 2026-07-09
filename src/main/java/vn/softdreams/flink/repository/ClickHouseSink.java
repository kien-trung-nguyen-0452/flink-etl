package vn.softdreams.flink.repository;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
public class ClickHouseSink extends RichSinkFunction<RepositoryLedgerRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSink.class);

    private final String host;
    private final int    port;
    private final String database;
    private final String user;
    private final String password;
    private final String table;
    private final int    batchSize;
    private final long   flushIntervalMs;

    private transient Connection         conn;
    private transient PreparedStatement  stmt;
    private transient List<RepositoryLedgerRecord> buffer;
    private transient long lastFlushTime;

    private static final String INSERT_SQL =
            "INSERT INTO %s " +
                    "(ID, CompanyID, BranchID, ReferenceID, Date, PostedDate, TypeLedger, " +
                    "NoFBook, NoMBook, Account, AccountCorresponding, " +
                    "RepositoryID, RepositoryCode, RepositoryName, " +
                    "MaterialGoodsID, MaterialGoodsCode, MaterialGoodsName, " +
                    "UnitID, UnitPrice, IWQuantity, OWQuantity, IWAmount, OWAmount, " +
                    "MainUnitID, MainUnitPrice, MainIWQuantity, MainOWQuantity, MainConvertRate, Formula, " +
                    "Reason, Description, ExpiryDate, LotNo, " +
                    "BudgetItemID, CostSetID, StatisticsCodeID, ExpenseItemID, " +
                    "DetailID, TypeID, OrderPriority, ConfrontID, ConfrontDetailID, " +
                    "IsPromotion, RefDateTime, DepartmentID, AccountingObjectID, ContractID, " +
                    "CustomField1, CustomField2, CustomField3, CustomField4, CustomField5, " +
                    "CustomFieldDetail1, CustomFieldDetail2, CustomFieldDetail3, CustomFieldDetail4, CustomFieldDetail5, " +
                    "created_date, RefID, " +
                    "__source_ts_ms, __deleted, cluster_id) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        // Native protocol settings
        props.setProperty("socket_timeout", "600000");
        props.setProperty("connect_timeout", "30000");
        props.setProperty("compress", "1");  // LZ4 compression

        String url = String.format("jdbc:clickhouse://%s:%d/%s", host, port, database);
        conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(false);

        stmt          = conn.prepareStatement(String.format(INSERT_SQL, table));
        buffer        = new ArrayList<>(batchSize);
        lastFlushTime = System.currentTimeMillis();

        LOG.info("ClickHouseSink opened | host={}:{} table={} batchSize={} flushInterval={}ms",
                host, port, table, batchSize, flushIntervalMs);
    }

    @Override
    public void invoke(RepositoryLedgerRecord r, Context context) throws Exception {
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
                bindRecord(stmt, r);
                stmt.addBatch();
                count++;
            }
            stmt.executeBatch();
            conn.commit();
            lastFlushTime = System.currentTimeMillis();
            LOG.debug("Flushed {} records to ClickHouse", count);
        } catch (Exception e) {
            LOG.error("Failed to flush {} records to ClickHouse", count, e);
            try { conn.rollback(); } catch (Exception ignored) {}
            reconnect();
            throw e;
        } finally {
            buffer.clear();
            stmt.clearBatch();
        }
    }

    private void bindRecord(PreparedStatement s, RepositoryLedgerRecord r) throws Exception {
        int i = 1;
        s.setString(i++, r.getId());
        s.setString(i++, r.getCompanyId());
        s.setString(i++, r.getBranchId());
        s.setString(i++, r.getReferenceId());
        s.setString(i++, r.getDate());
        s.setString(i++, r.getPostedDate());
        setNullableInt(s, i++, r.getTypeLedger());
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
        setNullableDecimal(s, i++, r.getUnitPrice());
        setNullableDecimal(s, i++, r.getIwQuantity());
        setNullableDecimal(s, i++, r.getOwQuantity());
        setNullableDecimal(s, i++, r.getIwAmount());
        setNullableDecimal(s, i++, r.getOwAmount());
        s.setString(i++, r.getMainUnitId());
        setNullableDecimal(s, i++, r.getMainUnitPrice());
        setNullableDecimal(s, i++, r.getMainIwQuantity());
        setNullableDecimal(s, i++, r.getMainOwQuantity());
        setNullableDecimal(s, i++, r.getMainConvertRate());
        s.setString(i++, r.getFormula());
        s.setString(i++, r.getReason());
        s.setString(i++, r.getDescription());
        s.setString(i++, r.getExpiryDate());
        s.setString(i++, r.getLotNo());
        s.setString(i++, r.getBudgetItemId());
        s.setString(i++, r.getCostSetId());
        s.setString(i++, r.getStatisticsCodeId());
        s.setString(i++, r.getExpenseItemId());
        s.setString(i++, r.getDetailId());
        setNullableInt(s, i++, r.getTypeId());
        setNullableInt(s, i++, r.getOrderPriority());
        s.setString(i++, r.getConfrontId());
        s.setString(i++, r.getConfrontDetailId());
        setNullableInt(s, i++, r.getIsPromotion());
        s.setString(i++, r.getRefDateTime());
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
        s.setString(i++, r.getCreatedDate());
        s.setString(i++, r.getRefId());
        s.setLong(i++, r.getSourceTs());
        s.setInt(i++, r.getDeleted());
        s.setString(i++, r.getClusterId());
    }

    private void setNullableInt(PreparedStatement s, int idx, Integer val) throws Exception {
        if (val == null) s.setNull(idx, java.sql.Types.INTEGER);
        else s.setInt(idx, val);
    }

    private void setNullableDecimal(PreparedStatement s, int idx, java.math.BigDecimal val) throws Exception {
        if (val == null) s.setNull(idx, java.sql.Types.DECIMAL);
        else s.setBigDecimal(idx, val);
    }

    private void reconnect() {
        LOG.warn("Reconnecting to ClickHouse...");
        try {
            if (conn != null && !conn.isClosed()) conn.close();
        } catch (Exception ignored) {}
        try {
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("socket_timeout", "600000");
            props.setProperty("compress", "1");
            String url = String.format("jdbc:clickhouse://%s:%d/%s", host, port, database);
            conn = DriverManager.getConnection(url, props);
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(String.format(INSERT_SQL, table));
            LOG.info("Reconnected to ClickHouse successfully");
        } catch (Exception e) {
            LOG.error("Failed to reconnect to ClickHouse", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } catch (Exception e) {
            LOG.error("Error during final flush", e);
        }
        if (stmt != null) try { stmt.close(); } catch (Exception ignored) {}
        if (conn != null) try { conn.close(); } catch (Exception ignored) {}
    }
}