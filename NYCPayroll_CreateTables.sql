-- Create Agency Dimension
CREATE TABLE "EDW"."Agency_DIM" (
    "AgencyID" INT NOT NULL PRIMARY KEY,
    "AgencyName" VARCHAR(250) NOT NULL
    -- "AgencyID" is the unique identifier
);

-- Create Payroll Dimension
CREATE TABLE "EDW"."Payroll_DIM"(
    "PayrollNumber" INT NOT NULL PRIMARY KEY,
    "AgencyID" INT NOT NULL,
    FOREIGN KEY ("AgencyID") REFERENCES "EDW"."Agency_DIM"("AgencyID")
);

-- Create Employee Dimension
CREATE TABLE "EDW"."Employee_DIM" (
    "EmployeeID" INT NOT NULL PRIMARY KEY,
    "FirstName" VARCHAR(250) NOT NULL,
    "LastName" VARCHAR(250) NOT NULL
);

-- Create Location Dimension
CREATE TABLE "EDW"."Location_DIM" (
    "LocationID" SERIAL PRIMARY KEY,
    "WorkLocationBorough" VARCHAR(250) NOT NULL UNIQUE -- Unique constraint
);

-- Create Title Dimension
CREATE TABLE "EDW"."Title_DIM" (
    "TitleCode" INT NOT NULL PRIMARY KEY,
    "TitleDescription" VARCHAR(250) NOT NULL
);

-- Create Leave-Status Dimension
CREATE TABLE "EDW"."LeaveStatus_DIM" (
    "LeaveStatusID" SERIAL PRIMARY KEY,
    "LeaveStatusasofJune30" VARCHAR(20) NOT NULL UNIQUE -- Unique constraint
);


-- Create Pay-Basis Dimension
CREATE TABLE "EDW"."PayBasis_DIM" (
    "PayBasisID" SERIAL PRIMARY KEY,
    "PayBasis" VARCHAR(20) NOT NULL UNIQUE -- Unique constraint
);


-- Create NYC Payroll-Fact Table
CREATE TABLE "EDW"."NYC_Payroll_Fact" (
    "PayrollID" VARCHAR(20) PRIMARY KEY, -- Unique payroll ID
    "FiscalYear" INT NOT NULL,
    "PayrollNumber" INT NOT NULL,
    "EmployeeID" INT NOT NULL,
    "AgencyID" INT NOT NULL,
    "AgencyStartDate" DATE NOT NULL,
    "LocationID" INT NOT NULL,
    "TitleCode" INT NOT NULL,
    "LeaveStatusID" INT NOT NULL,
    "BaseSalary" DECIMAL(10, 2) NOT NULL,
    "PayBasisID" INT NOT NULL,
    "RegularHours" DECIMAL(10, 2),
    "RegularGrossPaid" DECIMAL(10, 2),
    "OTHours" DECIMAL(10, 2),
    "TotalOTPaid" DECIMAL(10, 2),
    "TotalOtherPay" DECIMAL(10, 2),

    -- Foreign Keys
    FOREIGN KEY ("PayrollNumber") REFERENCES "EDW"."Payroll_DIM" ("PayrollNumber"),
    FOREIGN KEY ("EmployeeID") REFERENCES "EDW"."Employee_DIM" ("EmployeeID"),
    FOREIGN KEY ("AgencyID") REFERENCES "EDW"."Agency_DIM" ("AgencyID"),
    FOREIGN KEY ("LocationID") REFERENCES "EDW"."Location_DIM" ("LocationID"),
    FOREIGN KEY ("TitleCode") REFERENCES "EDW"."Title_DIM" ("TitleCode"),
    FOREIGN KEY ("LeaveStatusID") REFERENCES "EDW"."LeaveStatus_DIM" ("LeaveStatusID"),
    FOREIGN KEY ("PayBasisID") REFERENCES "EDW"."PayBasis_DIM" ("PayBasisID")
);

-- Create Aggregate Table 1 : Financial Allocation Summary by Agency
CREATE TABLE "EDW"."FinancialAllocationByAgency_agg" (
    "AgencyID" INT NOT NULL,
    "FiscalYear" INT NOT NULL,
    "TotalAmountPaid" DECIMAL(15, 2) NOT NULL,
    "TotalOvertimePaid" DECIMAL(15, 2) NOT NULL,
    "OvertimePercentage" DECIMAL(5, 2) NOT NULL,
    PRIMARY KEY ("AgencyID", "FiscalYear"),
    FOREIGN KEY ("AgencyID") REFERENCES "EDW"."Agency_DIM" ("AgencyID")
);

-- Create Aggregate Table 2 : Overtime Analysis by Employee
CREATE TABLE "EDW"."OvertimeByEmployee_agg" (
    "EmployeeID" INT NOT NULL,
    "FiscalYear" INT NOT NULL,
    "TotalOvertimeHours" DECIMAL(10, 2) NOT NULL,
    "TotalOvertimePaid" DECIMAL(15, 2) NOT NULL,
    PRIMARY KEY ("EmployeeID", "FiscalYear"),
    FOREIGN KEY ("EmployeeID") REFERENCES "EDW"."Employee_DIM" ("EmployeeID")
);


-- Create Aggregate Table 3 : Financial Summary by Location
CREATE TABLE "EDW"."FinancialSummaryByLocation_agg" (
    "LocationID" INT NOT NULL,
    "FiscalYear" INT NOT NULL,
    "TotalAmountPaid" DECIMAL(15, 2) NOT NULL,
    "TotalOvertimePaid" DECIMAL(15, 2) NOT NULL,
    PRIMARY KEY ("LocationID", "FiscalYear"),
    FOREIGN KEY ("LocationID") REFERENCES "EDW"."Location_DIM" ("LocationID")
);

-- Create Aggregate Table 4 : Financial Allocation by Pay Basis
CREATE TABLE "EDW"."FinancialByPayBasis_agg" (
    "PayBasisID" INT NOT NULL,
    "FiscalYear" INT NOT NULL,
    "TotalAmountPaid" DECIMAL(15, 2) NOT NULL,
    "TotalOvertimePaid" DECIMAL(15, 2) NOT NULL,
    PRIMARY KEY ("PayBasisID", "FiscalYear"),
    FOREIGN KEY ("PayBasisID") REFERENCES "EDW"."PayBasis_DIM" ("PayBasisID")
);

-- Create Aggregate Table 5 : Title-Based Payroll and Overtime Analysis
CREATE TABLE "EDW"."FinancialByTitle_agg" (
    "TitleCode" INT NOT NULL,
    "FiscalYear" INT NOT NULL,
    "TotalAmountPaid" DECIMAL(15, 2) NOT NULL,
    "TotalOvertimePaid" DECIMAL(15, 2) NOT NULL,
    "OvertimePercentage" DECIMAL(5, 2) NOT NULL,
    PRIMARY KEY ("TitleCode", "FiscalYear"),
    FOREIGN KEY ("TitleCode") REFERENCES "EDW"."Title_DIM" ("TitleCode")
);


--  Create Aggregate Table 6 : Annual Financial Summary
CREATE TABLE "EDW"."AnnualFinancialSummary_agg" (
    "FiscalYear" INT NOT NULL PRIMARY KEY,
    "TotalAmountPaid" DECIMAL(15, 2) NOT NULL,
    "TotalOvertimePaid" DECIMAL(15, 2) NOT NULL,
    "OvertimePercentage" DECIMAL(5, 2) NOT NULL
);





--  Create Procedure log Table for EDW Data Loading
CREATE TABLE IF NOT EXISTS "Staging"."EDW_DL_Procedure_Logs"(
	run_time TIMESTAMP,
	status TEXT,
	msg TEXT
);


--  Create Procedure log Table for Aggregate Table Creation
CREATE TABLE IF NOT EXISTS "Staging"."EDW_DAgg_Procedure_Logs"(
	run_time TIMESTAMP,
	status TEXT,
	msg TEXT
);


