package vn.softdreams.flink.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.math.BigDecimal;

@Slf4j
@RequiredArgsConstructor
public class RepositoryLedgerDeserializer implements DeserializationSchema<RepositoryLedgerRecord> {

    private static final long serialVersionUID = 1L;

    private final String clusterId;
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper();
    }

    @Override
    public RepositoryLedgerRecord deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) return null;

        JsonNode node;
        try {
            node = mapper.readTree(message);
        } catch (Exception e) {
            log.warn("Failed to parse JSON: {}", new String(message), e);
            return null;
        }

        if (node.isNull() || node.isEmpty()) return null;

        try {
            RepositoryLedgerRecord r = new RepositoryLedgerRecord();

            r.setId(str(node, "ID"));
            r.setCompanyId(str(node, "CompanyID"));
            r.setBranchId(str(node, "BranchID"));
            r.setReferenceId(str(node, "ReferenceID"));
            r.setDate(epochMs(node, "Date"));           // Long epoch ms
            r.setPostedDate(epochMs(node, "PostedDate")); // Long epoch ms
            r.setTypeLedger(intVal(node, "TypeLedger"));
            r.setNoFBook(str(node, "NoFBook"));
            r.setNoMBook(str(node, "NoMBook"));
            r.setAccount(str(node, "Account"));
            r.setAccountCorresponding(str(node, "AccountCorresponding"));

            r.setRepositoryId(str(node, "RepositoryID"));
            r.setRepositoryCode(str(node, "RepositoryCode"));
            r.setRepositoryName(str(node, "RepositoryName"));

            r.setMaterialGoodsId(str(node, "MaterialGoodsID"));
            r.setMaterialGoodsCode(str(node, "MaterialGoodsCode"));
            r.setMaterialGoodsName(str(node, "MaterialGoodsName"));

            r.setUnitId(str(node, "UnitID"));
            r.setUnitPrice(decimal(node, "UnitPrice"));
            r.setIwQuantity(decimal(node, "IWQuantity"));
            r.setOwQuantity(decimal(node, "OWQuantity"));
            r.setIwAmount(decimal(node, "IWAmount"));
            r.setOwAmount(decimal(node, "OWAmount"));
            r.setMainUnitId(str(node, "MainUnitID"));
            r.setMainUnitPrice(decimal(node, "MainUnitPrice"));
            r.setMainIwQuantity(decimal(node, "MainIWQuantity"));
            r.setMainOwQuantity(decimal(node, "MainOWQuantity"));
            r.setMainConvertRate(decimal(node, "MainConvertRate"));
            r.setFormula(str(node, "Formula"));

            r.setReason(str(node, "Reason"));
            r.setDescription(str(node, "Description"));
            r.setExpiryDate(epochMs(node, "ExpiryDate")); // Long epoch ms
            r.setLotNo(str(node, "LotNo"));

            r.setBudgetItemId(str(node, "BudgetItemID"));
            r.setCostSetId(str(node, "CostSetID"));
            r.setStatisticsCodeId(str(node, "StatisticsCodeID"));
            r.setExpenseItemId(str(node, "ExpenseItemID"));
            r.setDetailId(str(node, "DetailID"));
            r.setTypeId(intVal(node, "TypeID"));
            r.setOrderPriority(intVal(node, "OrderPriority"));
            r.setConfrontId(str(node, "ConfrontID"));
            r.setConfrontDetailId(str(node, "ConfrontDetailID"));
            r.setIsPromotion(boolVal(node, "IsPromotion"));
            r.setRefDateTime(epochMs(node, "RefDateTime")); // Long epoch ms
            r.setDepartmentId(str(node, "DepartmentID"));
            r.setAccountingObjectId(str(node, "AccountingObjectID"));
            r.setContractId(str(node, "ContractID"));

            r.setCustomField1(str(node, "CustomField1"));
            r.setCustomField2(str(node, "CustomField2"));
            r.setCustomField3(str(node, "CustomField3"));
            r.setCustomField4(str(node, "CustomField4"));
            r.setCustomField5(str(node, "CustomField5"));
            r.setCustomFieldDetail1(str(node, "CustomFieldDetail1"));
            r.setCustomFieldDetail2(str(node, "CustomFieldDetail2"));
            r.setCustomFieldDetail3(str(node, "CustomFieldDetail3"));
            r.setCustomFieldDetail4(str(node, "CustomFieldDetail4"));
            r.setCustomFieldDetail5(str(node, "CustomFieldDetail5"));

            r.setCreatedDate(epochMs(node, "created_date")); // Long epoch ms
            r.setRefId(str(node, "RefID"));

            // CDC metadata
            r.setSourceTs(longVal(node, "__source_ts_ms"));
            r.setDeleted("d".equals(str(node, "__op")) ? 1 : 0);
            r.setClusterId(clusterId);

            return r;

        } catch (Exception e) {
            log.error("Failed to map fields: {}", node, e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(RepositoryLedgerRecord r) { return false; }

    @Override
    public TypeInformation<RepositoryLedgerRecord> getProducedType() {
        return TypeInformation.of(RepositoryLedgerRecord.class);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** String field — trả null nếu absent/null */
    private static String str(JsonNode n, String f) {
        JsonNode v = n.get(f);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    /** Integer nullable */
    private static Integer intVal(JsonNode n, String f) {
        JsonNode v = n.get(f);
        return (v == null || v.isNull()) ? null : v.asInt();
    }

    /** bit/boolean → 0 or 1 */
    private static int boolVal(JsonNode n, String f) {
        JsonNode v = n.get(f);
        if (v == null || v.isNull()) return 0;
        return (v.asBoolean() || v.asInt() == 1) ? 1 : 0;
    }

    /** Long (non-nullable, default 0) */
    private static long longVal(JsonNode n, String f) {
        JsonNode v = n.get(f);
        return (v == null || v.isNull()) ? 0L : v.asLong();
    }

    /**
     * Debezium SQL Server datetime → epoch milliseconds (Long nullable).
     * Trả null nếu field absent hoặc null trong DB.
     */
    private static Long epochMs(JsonNode n, String f) {
        JsonNode v = n.get(f);
        return (v == null || v.isNull()) ? null : v.asLong();
    }

    /** Decimal/numeric field */
    private static BigDecimal decimal(JsonNode n, String f) {
        JsonNode v = n.get(f);
        if (v == null || v.isNull()) return null;
        try { return new BigDecimal(v.asText()); } catch (Exception e) { return null; }
    }
}