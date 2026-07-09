package vn.softdreams.flink.repository;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
public class RepositoryLedgerRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    // =========================
    // Business Keys
    // =========================

    @JsonProperty("ID")
    private String id;

    @JsonProperty("CompanyID")
    private String companyId;

    @JsonProperty("BranchID")
    private String branchId;

    @JsonProperty("ReferenceID")
    private String referenceId;

    @JsonProperty("Date")
    private String date;

    @JsonProperty("PostedDate")
    private String postedDate;

    @JsonProperty("TypeLedger")
    private Integer typeLedger;

    @JsonProperty("NoFBook")
    private String noFBook;

    @JsonProperty("NoMBook")
    private String noMBook;

    @JsonProperty("Account")
    private String account;

    @JsonProperty("AccountCorresponding")
    private String accountCorresponding;

    // =========================
    // Repository
    // =========================

    @JsonProperty("RepositoryID")
    private String repositoryId;

    @JsonProperty("RepositoryCode")
    private String repositoryCode;

    @JsonProperty("RepositoryName")
    private String repositoryName;

    // =========================
    // Material Goods
    // =========================

    @JsonProperty("MaterialGoodsID")
    private String materialGoodsId;

    @JsonProperty("MaterialGoodsCode")
    private String materialGoodsCode;

    @JsonProperty("MaterialGoodsName")
    private String materialGoodsName;

    // =========================
    // Unit
    // =========================

    @JsonProperty("UnitID")
    private String unitId;

    @JsonProperty("UnitPrice")
    private BigDecimal unitPrice;

    @JsonProperty("IWQuantity")
    private BigDecimal iwQuantity;

    @JsonProperty("OWQuantity")
    private BigDecimal owQuantity;

    @JsonProperty("IWAmount")
    private BigDecimal iwAmount;

    @JsonProperty("OWAmount")
    private BigDecimal owAmount;

    @JsonProperty("MainUnitID")
    private String mainUnitId;

    @JsonProperty("MainUnitPrice")
    private BigDecimal mainUnitPrice;

    @JsonProperty("MainIWQuantity")
    private BigDecimal mainIwQuantity;

    @JsonProperty("MainOWQuantity")
    private BigDecimal mainOwQuantity;

    @JsonProperty("MainConvertRate")
    private BigDecimal mainConvertRate;

    @JsonProperty("Formula")
    private String formula;

    // =========================
    // Description
    // =========================

    @JsonProperty("Reason")
    private String reason;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("ExpiryDate")
    private String expiryDate;

    @JsonProperty("LotNo")
    private String lotNo;

    // =========================
    // Reference
    // =========================

    @JsonProperty("BudgetItemID")
    private String budgetItemId;

    @JsonProperty("CostSetID")
    private String costSetId;

    @JsonProperty("StatisticsCodeID")
    private String statisticsCodeId;

    @JsonProperty("ExpenseItemID")
    private String expenseItemId;

    @JsonProperty("DetailID")
    private String detailId;

    @JsonProperty("TypeID")
    private Integer typeId;

    @JsonProperty("OrderPriority")
    private Integer orderPriority;

    @JsonProperty("ConfrontID")
    private String confrontId;

    @JsonProperty("ConfrontDetailID")
    private String confrontDetailId;

    @JsonProperty("IsPromotion")
    private Integer isPromotion;

    @JsonProperty("RefDateTime")
    private String refDateTime;

    @JsonProperty("DepartmentID")
    private String departmentId;

    @JsonProperty("AccountingObjectID")
    private String accountingObjectId;

    @JsonProperty("ContractID")
    private String contractId;

    // =========================
    // Custom Field
    // =========================

    @JsonProperty("CustomField1")
    private String customField1;

    @JsonProperty("CustomField2")
    private String customField2;

    @JsonProperty("CustomField3")
    private String customField3;

    @JsonProperty("CustomField4")
    private String customField4;

    @JsonProperty("CustomField5")
    private String customField5;

    @JsonProperty("CustomFieldDetail1")
    private String customFieldDetail1;

    @JsonProperty("CustomFieldDetail2")
    private String customFieldDetail2;

    @JsonProperty("CustomFieldDetail3")
    private String customFieldDetail3;

    @JsonProperty("CustomFieldDetail4")
    private String customFieldDetail4;

    @JsonProperty("CustomFieldDetail5")
    private String customFieldDetail5;

    @JsonProperty("created_date")
    private String createdDate;

    @JsonProperty("RefID")
    private String refId;

    // =========================
    // CDC Metadata
    // =========================

    @JsonProperty("__source_ts_ms")
    private long sourceTs;
    private int deleted;
    private String clusterId;
}