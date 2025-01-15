CREATE OR REPLACE PROCEDURE "Staging"."prc_agg_NYCPayrollData"()
LANGUAGE "plpgsql"
AS
$BODY$
DECLARE
    v_runtime TIMESTAMP;
    v_status TEXT;
    v_msg TEXT;
BEGIN
    v_runtime := NOW();
    v_status := 'SUCCESS';
    v_msg := NULL;

    -- Populate FinancialAllocationByAgency Aggregate Table
    INSERT INTO "EDW"."FinancialAllocationByAgency_agg" ("AgencyID", "FiscalYear", "TotalAmountPaid", "TotalOvertimePaid", "OvertimePercentage")
    SELECT 
        "AgencyID",
        "FiscalYear",
        SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") AS "TotalAmountPaid",
        SUM("TotalOTPaid") AS "TotalOvertimePaid",
        CASE 
            WHEN SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") = 0 THEN 0
            ELSE ROUND(SUM("TotalOTPaid") * 100.0 / SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay"), 2)
        END AS "OvertimePercentage"
    FROM "EDW"."NYC_Payroll_Fact"
    GROUP BY "AgencyID", "FiscalYear"
    ON CONFLICT ("AgencyID", "FiscalYear") DO UPDATE
    SET 
        "TotalAmountPaid" = EXCLUDED."TotalAmountPaid",
        "TotalOvertimePaid" = EXCLUDED."TotalOvertimePaid",
        "OvertimePercentage" = EXCLUDED."OvertimePercentage";

    -- Populate OvertimeByEmployee Aggregate Table
    INSERT INTO "EDW"."OvertimeByEmployee_agg" ("EmployeeID", "FiscalYear", "TotalOvertimeHours", "TotalOvertimePaid")
    SELECT 
        "EmployeeID",
        "FiscalYear",
        SUM("OTHours") AS "TotalOvertimeHours",
        SUM("TotalOTPaid") AS "TotalOvertimePaid"
    FROM "EDW"."NYC_Payroll_Fact"
    GROUP BY "EmployeeID", "FiscalYear"
    ON CONFLICT ("EmployeeID", "FiscalYear") DO UPDATE
    SET 
        "TotalOvertimeHours" = EXCLUDED."TotalOvertimeHours",
        "TotalOvertimePaid" = EXCLUDED."TotalOvertimePaid";

    -- Populate FinancialSummaryByLocation Aggregate Table
    INSERT INTO "EDW"."FinancialSummaryByLocation_agg" ("LocationID", "FiscalYear", "TotalAmountPaid", "TotalOvertimePaid")
    SELECT 
        "LocationID",
        "FiscalYear",
        SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") AS "TotalAmountPaid",
        SUM("TotalOTPaid") AS "TotalOvertimePaid"
    FROM "EDW"."NYC_Payroll_Fact"
    GROUP BY "LocationID", "FiscalYear"
    ON CONFLICT ("LocationID", "FiscalYear") DO UPDATE
    SET 
        "TotalAmountPaid" = EXCLUDED."TotalAmountPaid",
        "TotalOvertimePaid" = EXCLUDED."TotalOvertimePaid";

    -- Populate FinancialByPayBasis Aggregate Table
    INSERT INTO "EDW"."FinancialByPayBasis_agg" ("PayBasisID", "FiscalYear", "TotalAmountPaid", "TotalOvertimePaid")
    SELECT 
        "PayBasisID",
        "FiscalYear",
        SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") AS "TotalAmountPaid",
        SUM("TotalOTPaid") AS "TotalOvertimePaid"
    FROM "EDW"."NYC_Payroll_Fact"
    GROUP BY "PayBasisID", "FiscalYear"
    ON CONFLICT ("PayBasisID", "FiscalYear") DO UPDATE
    SET 
        "TotalAmountPaid" = EXCLUDED."TotalAmountPaid",
        "TotalOvertimePaid" = EXCLUDED."TotalOvertimePaid";

    -- Populate FinancialByTitle Aggregate Table
    INSERT INTO "EDW"."FinancialByTitle_agg" ("TitleCode", "FiscalYear", "TotalAmountPaid", "TotalOvertimePaid", "OvertimePercentage")
    SELECT 
        "TitleCode",
        "FiscalYear",
        SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") AS "TotalAmountPaid",
        SUM("TotalOTPaid") AS "TotalOvertimePaid",
        CASE 
            WHEN SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") = 0 THEN 0
            ELSE ROUND(SUM("TotalOTPaid") * 100.0 / SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay"), 2)
        END AS "OvertimePercentage"
    FROM "EDW"."NYC_Payroll_Fact"
    GROUP BY "TitleCode", "FiscalYear"
    ON CONFLICT ("TitleCode", "FiscalYear") DO UPDATE
    SET 
        "TotalAmountPaid" = EXCLUDED."TotalAmountPaid",
        "TotalOvertimePaid" = EXCLUDED."TotalOvertimePaid",
        "OvertimePercentage" = EXCLUDED."OvertimePercentage";

    -- Populate AnnualFinancialSummary Aggregate Table
    INSERT INTO "EDW"."AnnualFinancialSummary_agg" ("FiscalYear", "TotalAmountPaid", "TotalOvertimePaid", "OvertimePercentage")
    SELECT 
        "FiscalYear",
        SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") AS "TotalAmountPaid",
        SUM("TotalOTPaid") AS "TotalOvertimePaid",
        CASE 
            WHEN SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay") = 0 THEN 0
            ELSE ROUND(SUM("TotalOTPaid") * 100.0 / SUM("RegularGrossPaid" + "TotalOTPaid" + "TotalOtherPay"), 2)
        END AS "OvertimePercentage"
    FROM "EDW"."NYC_Payroll_Fact"
    GROUP BY "FiscalYear"
    ON CONFLICT ("FiscalYear") DO UPDATE
    SET 
        "TotalAmountPaid" = EXCLUDED."TotalAmountPaid",
        "TotalOvertimePaid" = EXCLUDED."TotalOvertimePaid",
        "OvertimePercentage" = EXCLUDED."OvertimePercentage";

    -- Logging outcome
    INSERT INTO "Staging"."EDW_DAgg_Procedure_Logs"(run_time, status, msg)
    VALUES (v_runtime, v_status, v_msg);

EXCEPTION
    WHEN OTHERS THEN
        v_status := 'FAILED';
        v_msg := SQLERRM;
        
        INSERT INTO "Staging"."EDW_DAgg_Procedure_Logs"(run_time, status, msg)
        VALUES (v_runtime, v_status, v_msg);

END;
$BODY$;