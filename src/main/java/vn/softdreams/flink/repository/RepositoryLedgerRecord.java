package vn.softdreams.flink.repository;

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

    // Business keys
    private String  id;
    private String  companyId;
    private String  branchId;
    private String  referenceId;
    private Long    date;         // epoch ms
    private Long    postedDate;   // epoch ms
    private Integer typeLedger;
    private String  noFBook;
    private String  noMBook;
    private String  account;
    private String  accountCorresponding;

    // Repository
    private String repositoryId;
    private String repositoryCode;
    private String repositoryName;

    // Material goods
    private String materialGoodsId;
    private String materialGoodsCode;
    private String materialGoodsName;

    // Units & quantities
    private String     unitId;
    private BigDecimal unitPrice;
    private BigDecimal iwQuantity;
    private BigDecimal owQuantity;
    private BigDecimal iwAmount;
    private BigDecimal owAmount;
    private String     mainUnitId;
    private BigDecimal mainUnitPrice;
    private BigDecimal mainIwQuantity;
    private BigDecimal mainOwQuantity;
    private BigDecimal mainConvertRate;
    private String     formula;

    // Description
    private String reason;
    private String description;
    private Long   expiryDate;    // epoch ms
    private String lotNo;

    // References
    private String  budgetItemId;
    private String  costSetId;
    private String  statisticsCodeId;
    private String  expenseItemId;
    private String  detailId;
    private Integer typeId;
    private Integer orderPriority;
    private String  confrontId;
    private String  confrontDetailId;
    private Integer isPromotion;
    private Long    refDateTime;   // epoch ms
    private String  departmentId;
    private String  accountingObjectId;
    private String  contractId;

    // Custom fields
    private String customField1;
    private String customField2;
    private String customField3;
    private String customField4;
    private String customField5;
    private String customFieldDetail1;
    private String customFieldDetail2;
    private String customFieldDetail3;
    private String customFieldDetail4;
    private String customFieldDetail5;

    private Long   createdDate;   // epoch ms
    private String refId;

    // CDC metadata
    private long   sourceTs;   // __source_ts_ms từ Debezium
    private int    deleted;    // 0 = alive, 1 = deleted
    private String clusterId;
}