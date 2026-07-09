package vn.softdreams.flink.repository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
public class RepositoryLedgerDeserializer implements DeserializationSchema<RepositoryLedgerRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryLedgerDeserializer.class);

    private final String clusterId;
    private transient ObjectMapper mapper;

    public RepositoryLedgerDeserializer(String clusterId) {
        this.clusterId = clusterId;
    }

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
            LOG.warn("Failed to parse JSON message: {}", new String(message), e);
            return null;
        }

        // Skip tombstone messages
        if (node.isNull() || node.isEmpty()) return null;

        try {
            RepositoryLedgerRecord r = new RepositoryLedgerRecord();

            r.setId(text(node, "ID"));
            r.setCompanyId(text(node, "CompanyID"));
            r.setBranchId(text(node, "BranchID"));
            r.setReferenceId(text(node, "ReferenceID"));
            r.setDate(text(node, "Date"));
            r.setPostedDate(text(node, "PostedDate"));
            r.setTypeLedger(intVal(node, "TypeLedger"));
            r.setNoFBook(text(node, "NoFBook"));
            r.setNoMBook(text(node, "NoMBook"));
            r.setAccount(text(node, "Account"));
            r.setAccountCorresponding(text(node, "AccountCorresponding"));

            r.setRepositoryId(text(node, "RepositoryID"));
            r.setRepositoryCode(text(node, "RepositoryCode"));
            r.setRepositoryName(text(node, "RepositoryName"));

            r.setMaterialGoodsId(text(node, "MaterialGoodsID"));
            r.setMaterialGoodsCode(text(node, "MaterialGoodsCode"));
            r.setMaterialGoodsName(text(node, "MaterialGoodsName"));

            r.setUnitId(text(node, "UnitID"));
            r.setUnitPrice(decimal(node, "UnitPrice"));
            r.setIwQuantity(decimal(node, "IWQuantity"));
            r.setOwQuantity(decimal(node, "OWQuantity"));
            r.setIwAmount(decimal(node, "IWAmount"));
            r.setOwAmount(decimal(node, "OWAmount"));
            r.setMainUnitId(text(node, "MainUnitID"));
            r.setMainUnitPrice(decimal(node, "MainUnitPrice"));
            r.setMainIwQuantity(decimal(node, "MainIWQuantity"));
            r.setMainOwQuantity(decimal(node, "MainOWQuantity"));
            r.setMainConvertRate(decimal(node, "MainConvertRate"));
            r.setFormula(text(node, "Formula"));

            r.setReason(text(node, "Reason"));
            r.setDescription(text(node, "Description"));
            r.setExpiryDate(text(node, "ExpiryDate"));
            r.setLotNo(text(node, "LotNo"));

            r.setBudgetItemId(text(node, "BudgetItemID"));
            r.setCostSetId(text(node, "CostSetID"));
            r.setStatisticsCodeId(text(node, "StatisticsCodeID"));
            r.setExpenseItemId(text(node, "ExpenseItemID"));
            r.setDetailId(text(node, "DetailID"));
            r.setTypeId(intVal(node, "TypeID"));
            r.setOrderPriority(intVal(node, "OrderPriority"));
            r.setConfrontId(text(node, "ConfrontID"));
            r.setConfrontDetailId(text(node, "ConfrontDetailID"));
            r.setIsPromotion(boolVal(node, "IsPromotion"));
            r.setRefDateTime(text(node, "RefDateTime"));
            r.setDepartmentId(text(node, "DepartmentID"));
            r.setAccountingObjectId(text(node, "AccountingObjectID"));
            r.setContractId(text(node, "ContractID"));

            r.setCustomField1(text(node, "CustomField1"));
            r.setCustomField2(text(node, "CustomField2"));
            r.setCustomField3(text(node, "CustomField3"));
            r.setCustomField4(text(node, "CustomField4"));
            r.setCustomField5(text(node, "CustomField5"));
            r.setCustomFieldDetail1(text(node, "CustomFieldDetail1"));
            r.setCustomFieldDetail2(text(node, "CustomFieldDetail2"));
            r.setCustomFieldDetail3(text(node, "CustomFieldDetail3"));
            r.setCustomFieldDetail4(text(node, "CustomFieldDetail4"));
            r.setCustomFieldDetail5(text(node, "CustomFieldDetail5"));

            r.setCreatedDate(text(node, "created_date"));
            r.setRefId(text(node, "RefID"));

            // CDC metadata
            r.setSourceTs(longVal(node, "__source_ts_ms"));
            r.setDeleted("d".equals(text(node, "__op")) ? 1 : 0);
            r.setClusterId(clusterId);
            return r;
        } catch (Exception e) {
            LOG.error("Failed to map fields from JSON: {}", node, e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(RepositoryLedgerRecord record) {
        return false;
    }

    @Override
    public TypeInformation<RepositoryLedgerRecord> getProducedType() {
        return TypeInformation.of(RepositoryLedgerRecord.class);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String text(JsonNode node, String field) {
        JsonNode f = node.get(field);
        return (f == null || f.isNull()) ? null : f.asText();
    }

    private static Integer intVal(JsonNode node, String field) {
        JsonNode f = node.get(field);
        return (f == null || f.isNull()) ? null : f.asInt();
    }

    private static int boolVal(JsonNode node, String field) {
        JsonNode f = node.get(field);
        if (f == null || f.isNull()) return 0;
        return (f.asBoolean() || f.asInt() == 1) ? 1 : 0;
    }

    private static long longVal(JsonNode node, String field) {
        JsonNode f = node.get(field);
        return (f == null || f.isNull()) ? 0L : f.asLong();
    }

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode f = node.get(field);
        if (f == null || f.isNull()) return null;
        try {
            return new BigDecimal(f.asText());
        } catch (Exception e) {
            return null;
        }
    }
}