-- ============================================
-- Snowflake Setup Script for IPEDS Data
-- Texas Community College Benchmarking Project
-- ============================================

-- 0. CREATE WAREHOUSE (if needed)
-- ============================================
-- Create a small warehouse for development
-- For production, consider larger sizes or auto-scaling
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for IPEDS data processing';

USE WAREHOUSE COMPUTE_WH;

-- 1. CREATE DATABASE AND SCHEMAS
-- ============================================
CREATE DATABASE IF NOT EXISTS TEXAS_CC
    COMMENT = 'Texas Community College Benchmarking Database';

USE DATABASE TEXAS_CC;

-- Create schemas following dbt best practices
CREATE SCHEMA IF NOT EXISTS RAW_IPEDS
    COMMENT = 'Raw IPEDS data from downloads';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Staging area for initial transformations';

CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'Intermediate models (dbt)';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Final analytical marts for reporting';

USE SCHEMA RAW_IPEDS;

-- 2. CREATE FILE FORMAT
-- ============================================
CREATE OR REPLACE FILE FORMAT ipeds_csv
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', '.')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- 3. CREATE STAGE
-- ============================================
CREATE OR REPLACE STAGE ipeds_stage
    FILE_FORMAT = ipeds_csv;

-- 4. RAW TABLES
-- ============================================
-- Adding metadata columns (_loaded_at, _source_file) for data lineage

-- HD: Institutional Characteristics (Directory)
-- This is the core directory file with institution information
CREATE OR REPLACE TABLE HD (
    UNITID NUMBER PRIMARY KEY,
    INSTNM VARCHAR(500),
    IALIAS VARCHAR(500),
    CITY VARCHAR(100),
    STABBR VARCHAR(10),
    ZIP VARCHAR(20),
    FIPS NUMBER,
    OBEREG NUMBER,
    SECTOR NUMBER,  -- Important: 4 = Public 2-year
    ICLEVEL NUMBER,
    CONTROL NUMBER, -- 1=Public, 2=Private nonprofit, 3=Private for-profit
    HLOFFER NUMBER,
    UGOFFER NUMBER,
    GROFFER NUMBER,
    LOCALE NUMBER,
    INSTSIZE NUMBER,
    C18BASIC NUMBER,
    C18SZSET NUMBER,
    ACCREDAGENCY VARCHAR(500),
    WEBADDR VARCHAR(500),
    ADMINURL VARCHAR(500),
    FAIDURL VARCHAR(500),
    APPLURL VARCHAR(500),
    -- Metadata
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- C_A: Completions by Award Level
-- Degrees and certificates awarded by CIP code
CREATE OR REPLACE TABLE C_A (
    UNITID NUMBER,
    CIPCODE VARCHAR(20),  -- Classification of Instructional Programs code
    AWLEVEL NUMBER,       -- Award level (1=Certificate, 3=Associate, etc.)
    MAJORNUM NUMBER,
    CTOTALT NUMBER,       -- Total completions
    CTOTALM NUMBER,       -- Male completions
    CTOTALW NUMBER,       -- Female completions
    CAIANT NUMBER,
    CASIAT NUMBER,
    CBKAAT NUMBER,
    CHISPT NUMBER,
    CNHPIT NUMBER,
    CWHITT NUMBER,
    C2MORT NUMBER,
    CUNKNT NUMBER,
    CNRALT NUMBER,
    -- Metadata
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- EFFY: 12-Month Enrollment
-- Unduplicated headcount over 12-month period (July 1 - June 30)
CREATE OR REPLACE TABLE EFFY (
    UNITID NUMBER,
    EFFYALEV NUMBER,   -- Award level
    EFYTOTLT NUMBER,   -- Total 12-month enrollment
    EFYTOTLM NUMBER,   -- Male enrollment
    EFYTOTLW NUMBER,   -- Female enrollment
    EFYAIANT NUMBER,
    EFYASIAT NUMBER,
    EFYBKAAT NUMBER,
    EFYHISPT NUMBER,
    EFYNHPIT NUMBER,
    EFYWHITT NUMBER,
    EFY2MORT NUMBER,
    EFYUNKNT NUMBER,
    EFYNRALT NUMBER,
    -- Metadata
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- GR: Graduation Rates
-- Graduation rates for first-time, full-time degree-seeking students
CREATE OR REPLACE TABLE GR (
    UNITID NUMBER,
    CHRTSTAT NUMBER,   -- Cohort status
    SECTION NUMBER,
    COHESSION NUMBER,  -- Cohort year
    GRTYPE NUMBER,     -- Graduation rate type
    GRTOTLT NUMBER,    -- Total graduation rate
    GRTOTLM NUMBER,    -- Male graduation rate
    GRTOTLW NUMBER,    -- Female graduation rate
    GRTOTAT NUMBER,
    GRTOTAM NUMBER,
    GRTOTAW NUMBER,
    GRENRLT NUMBER,
    GRAIANTT NUMBER,
    GRASIATT NUMBER,
    GRBKAATT NUMBER,
    GRHISPTT NUMBER,
    GRNHPITT NUMBER,
    GRWHITTT NUMBER,
    GR2MORTT NUMBER,
    GRAIANAT NUMBER,
    GRASIANAT NUMBER,
    GRBKAANAT NUMBER,
    GRHISPAN NUMBER,
    GRNHPIAT NUMBER,
    GRWHITAT NUMBER,
    GR2MORAT NUMBER,
    -- Metadata
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- SFA: Student Financial Aid
-- Financial aid information for undergraduate students
CREATE OR REPLACE TABLE SFA (
    UNITID NUMBER,
    SCUGRAD NUMBER,    -- Number of undergraduates analyzed
    UPGRNTN NUMBER,    -- Number receiving Pell grants
    UPGRNTA NUMBER,    -- Average Pell grant amount
    UAGRNTN NUMBER,    -- Number receiving federal grants
    UAGRNTT NUMBER,    -- Total federal grant amount
    UAGRNTA NUMBER,    -- Average federal grant amount
    USGRNTN NUMBER,    -- Number receiving state grants
    USGRNTA NUMBER,    -- Average state grant amount
    UIGRNTN NUMBER,    -- Number receiving institutional grants
    UIGRNTA NUMBER,    -- Average institutional grant amount
    UGRNT_N NUMBER,    -- Total receiving any grant
    UGRNT_A NUMBER,    -- Average grant amount
    UFLOANN NUMBER,    -- Number receiving federal loans
    UFLOANA NUMBER,    -- Average federal loan amount
    NPIST1 NUMBER,     -- Net price $0-30,000 income
    NPIST2 NUMBER,     -- Net price $30,001-48,000 income
    NPIST3 NUMBER,     -- Net price $48,001-75,000 income
    NPIST4 NUMBER,     -- Net price $75,001-110,000 income
    NPIST5 NUMBER,     -- Net price $110,001+ income
    -- Metadata
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);


-- 5. VERIFY SETUP
-- ============================================
-- List all tables in the RAW_IPEDS schema
SHOW TABLES IN SCHEMA TEXAS_CC.RAW_IPEDS;

-- Expected output: HD, C_A, EFFY, GR, SFA (all empty until data is loaded)