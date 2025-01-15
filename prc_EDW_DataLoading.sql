CREATE OR REPLACE PROCEDURE "Staging"."prc_EDW_DataLoading"()
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

    --- INSERT LOGIC

    -- Populate Agency Dimension
    INSERT INTO "EDW"."Agency_DIM" ("AgencyID", "AgencyName")
    SELECT DISTINCT 
        "AgencyID", 
        "AgencyName"
    FROM "Staging"."AgencyMaster"
    ON CONFLICT ("AgencyID") DO UPDATE
    SET 
        "AgencyName" = EXCLUDED."AgencyName";

    -- Populate Employee Dimension
    INSERT INTO "EDW"."Employee_DIM" ("EmployeeID", "FirstName", "LastName")
    SELECT DISTINCT 
        "EmployeeID", 
        "FirstName", 
        "LastName"
    FROM "Staging"."EmployeeMaster"
    ON CONFLICT ("EmployeeID") DO UPDATE
    SET 
        "FirstName" = EXCLUDED."FirstName",
        "LastName" = EXCLUDED."LastName";

    -- Populate Location Dimension
    INSERT INTO "EDW"."Location_DIM" ("WorkLocationBorough")
    SELECT DISTINCT "WorkLocationBorough"
    FROM "Staging"."NYCpayrollData"
    ON CONFLICT ("WorkLocationBorough") DO NOTHING;

    -- Populate Title Dimension
    INSERT INTO "EDW"."Title_DIM" ("TitleCode", "TitleDescription")
    SELECT DISTINCT 
        "TitleCode", 
        "TitleDescription"
    FROM "Staging"."TitleMaster"
    ON CONFLICT ("TitleCode") DO UPDATE
    SET 
        "TitleDescription" = EXCLUDED."TitleDescription";

    -- Populate Leave-Status Dimension
    INSERT INTO "EDW"."LeaveStatus_DIM" ("LeaveStatusasofJune30")
    SELECT DISTINCT "LeaveStatusasofJune30"
    FROM "Staging"."NYCpayrollData"
    ON CONFLICT ("LeaveStatusasofJune30") DO NOTHING;

    -- Populate Pay-Basis Dimension
    INSERT INTO "EDW"."PayBasis_DIM" ("PayBasis")
    SELECT DISTINCT "PayBasis"
    FROM "Staging"."NYCpayrollData"
    ON CONFLICT ("PayBasis") DO NOTHING;

    -- Populate Payroll Dimension
    INSERT INTO "EDW"."Payroll_DIM" ("PayrollNumber", "AgencyID")
    SELECT DISTINCT 
        "PayrollNumber",
        "AgencyID"
    FROM "Staging"."NYCpayrollData"
    ON CONFLICT ("PayrollNumber") DO NOTHING;

    -- Populate NYC_Payroll_Fact Table
    WITH OrderedData AS (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY "FiscalYear", "PayrollNumber" ORDER BY E."EmployeeID") AS "SerialNumber",
            S."FiscalYear",
            S."PayrollNumber",
            E."EmployeeID",
            A."AgencyID",
            S."AgencyStartDate",
            L."LocationID",
            T."TitleCode",
            LS."LeaveStatusID",
            S."BaseSalary",
            PB."PayBasisID",
            S."RegularHours",
            S."RegularGrossPaid",
            S."OTHours",
            S."TotalOTPaid",
            S."TotalOtherPay"
        FROM "Staging"."NYCpayrollData" S
        -- Join Employee Dimension
        LEFT JOIN "EDW"."Employee_DIM" E
            ON S."EmployeeID" = E."EmployeeID"
        -- Join Agency Dimension
        LEFT JOIN "EDW"."Agency_DIM" A
            ON S."AgencyID" = A."AgencyID"
        -- Join Location Dimension
        LEFT JOIN "EDW"."Location_DIM" L
            ON S."WorkLocationBorough" = L."WorkLocationBorough"
        -- Join Title Dimension
        LEFT JOIN "EDW"."Title_DIM" T
            ON S."TitleCode" = T."TitleCode"
        -- Join Leave-Status Dimension
        LEFT JOIN "EDW"."LeaveStatus_DIM" LS
            ON S."LeaveStatusasofJune30" = LS."LeaveStatusasofJune30"
        -- Join Pay-Basis Dimension
        LEFT JOIN "EDW"."PayBasis_DIM" PB
            ON S."PayBasis" = PB."PayBasis"
    )
    INSERT INTO "EDW"."NYC_Payroll_Fact" (
        "PayrollID",
        "FiscalYear",
        "PayrollNumber",
        "EmployeeID",
        "AgencyID",
        "AgencyStartDate",
        "LocationID",
        "TitleCode",
        "LeaveStatusID",
        "BaseSalary",
        "PayBasisID",
        "RegularHours",
        "RegularGrossPaid",
        "OTHours",
        "TotalOTPaid",
        "TotalOtherPay"
    )
    SELECT 
        CAST("FiscalYear" AS VARCHAR) || 
        CAST("PayrollNumber" AS VARCHAR) ||
        LPAD(CAST("SerialNumber" AS VARCHAR), 4, '0') AS "PayrollID", -- Generate PayrollID
        "FiscalYear",
        "PayrollNumber",
        "EmployeeID",
        "AgencyID",
        "AgencyStartDate",
        "LocationID",
        "TitleCode",
        "LeaveStatusID",
        "BaseSalary",
        "PayBasisID",
        "RegularHours",
        "RegularGrossPaid",
        "OTHours",
        "TotalOTPaid",
        "TotalOtherPay"
    FROM OrderedData
    ON CONFLICT ("PayrollID") DO UPDATE
    SET 
        "FiscalYear" = EXCLUDED."FiscalYear",
        "PayrollNumber" = EXCLUDED."PayrollNumber",
        "EmployeeID" = EXCLUDED."EmployeeID",
        "AgencyID" = EXCLUDED."AgencyID",
        "AgencyStartDate" = EXCLUDED."AgencyStartDate",
        "LocationID" = EXCLUDED."LocationID",
        "TitleCode" = EXCLUDED."TitleCode",
        "LeaveStatusID" = EXCLUDED."LeaveStatusID",
        "BaseSalary" = EXCLUDED."BaseSalary",
        "PayBasisID" = EXCLUDED."PayBasisID",
        "RegularHours" = EXCLUDED."RegularHours",
        "RegularGrossPaid" = EXCLUDED."RegularGrossPaid",
        "OTHours" = EXCLUDED."OTHours",
        "TotalOTPaid" = EXCLUDED."TotalOTPaid",
        "TotalOtherPay" = EXCLUDED."TotalOtherPay";

    --- LOGGING OUTCOME
    INSERT INTO "Staging"."EDW_DL_Procedure_Logs"(run_time, status, msg)
    VALUES (v_runtime, v_status, v_msg);

EXCEPTION 
    WHEN OTHERS THEN
        v_status := 'FAILED';
        v_msg := SQLERRM;
        
        INSERT INTO "Staging"."EDW_DL_Procedure_Logs"(run_time, status, msg)
        VALUES (v_runtime, v_status, v_msg);
    
END;
$BODY$;