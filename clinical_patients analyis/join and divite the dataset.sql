USE intern;

CREATE TABLE DIM_PATIENTS (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    patient_id VARCHAR(50) UNIQUE NOT NULL,
    age INT,
    gender VARCHAR(10),
    ethnicity VARCHAR(50),
    age_group VARCHAR(10),
    has_diabetes VARCHAR(10),
    has_hypertension VARCHAR(10),
    has_heart_disease VARCHAR(10),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO DIM_PATIENTS (patient_id, age, gender, ethnicity, age_group, has_diabetes, has_hypertension, has_heart_disease)
SELECT DISTINCT 
    patient_id,
    age,
    gender,
    ethnicity,
    age_group,
    has_diabetes,
    has_hypertension,
    has_heart_disease
FROM raw_dataset;

-- 2. Dimension: Sites
CREATE TABLE DIM_SITE (
  site_key      INT AUTO_INCREMENT PRIMARY KEY,
  site_id       VARCHAR(20) UNIQUE NOT NULL,
  site_name     VARCHAR(100),
  country       VARCHAR(50),
  city          VARCHAR(50)
);

INSERT INTO DIM_SITE (site_id, site_name, country, city)
SELECT DISTINCT
  site_id,
  site_name,
  country,
  city
FROM raw_dataset;
-- failed

-- 2. Dimension: Sites



-- Create DIM_SITE table if not exists
CREATE TABLE IF NOT EXISTS DIM_SITE (
  site_key   INT AUTO_INCREMENT PRIMARY KEY,
  site_id    VARCHAR(20) ,
  site_name  VARCHAR(100),
  country    VARCHAR(50),
  city       VARCHAR(50)
);

-- Insert new unique site records from raw_dataset
INSERT INTO DIM_SITE (site_id, site_name, country, city)
SELECT DISTINCT
  site_id,
  site_name,
  country,
  city
FROM raw_dataset
WHERE site_id NOT IN (SELECT site_id FROM DIM_SITE);

select * from DIM_SITE;

-- 3. Dimension: Dates
CREATE TABLE DIM_DATE (
  date_key      INT AUTO_INCREMENT PRIMARY KEY,
  full_date     DATE ,
  year          INT,
  quarter       INT,
  month         INT,
  day           INT,
  month_name    VARCHAR(10),
  day_name      VARCHAR(10)
);

-- drop table DIM_DATE ;


INSERT INTO DIM_DATE (full_date, year, quarter, month, day, month_name, day_name) 
SELECT DISTINCT
  d AS full_date,
  YEAR(d),
  QUARTER(d),
  MONTH(d),
  DAY(d),
  MONTHNAME(d),
  DAYNAME(d)
FROM (
  SELECT screening_date AS d FROM raw_dataset WHERE screening_date IS NOT NULL AND screening_date != ''
  UNION
  SELECT enrollment_date FROM raw_dataset WHERE enrollment_date IS NOT NULL AND enrollment_date != ''
  UNION
  SELECT randomization_date FROM raw_dataset WHERE randomization_date IS NOT NULL AND randomization_date != ''
) x;

select * from dim_date;

CREATE TABLE IF NOT EXISTS FACT_TRIAL_EVENTS (
  event_key               INT AUTO_INCREMENT primary key,
  patient_key             INT,
  site_key                INT,
  screening_date_key      INT,
  enrollment_date_key     INT,
  randomization_date_key  INT,
  event_status            VARCHAR(20),
  FOREIGN KEY (patient_key)            REFERENCES DIM_PATIENTS(patient_key),
  FOREIGN KEY (site_key)               REFERENCES DIM_SITE(site_key),
  FOREIGN KEY (screening_date_key)     REFERENCES DIM_DATE(date_key),
  FOREIGN KEY (enrollment_date_key)    REFERENCES DIM_DATE(date_key),
  FOREIGN KEY (randomization_date_key) REFERENCES DIM_DATE(date_key)
) ;


INSERT INTO FACT_TRIAL_EVENTS (
  patient_key, site_key,
  screening_date_key, enrollment_date_key, randomization_date_key,
  event_status
)
SELECT 
  p.patient_key,
  s.site_key,
  d1.date_key,
  d2.date_key,
  d3.date_key,
  CASE
    WHEN r.randomization_date IS NOT NULL AND r.randomization_date != '' THEN 'Randomized'
    WHEN r.enrollment_date IS NOT NULL AND r.enrollment_date != '' THEN 'Enrolled'
    ELSE 'Screened'
  END AS event_status
FROM (
  SELECT * FROM raw_dataset 
  WHERE (screening_date IS NULL OR screening_date != '')
    AND (enrollment_date IS NULL OR enrollment_date != '')
    AND (randomization_date IS NULL OR randomization_date != '')
) r
JOIN DIM_PATIENTS p ON r.patient_id = p.patient_id
JOIN DIM_SITE s ON r.site_id = s.site_id
LEFT JOIN DIM_DATE d1 ON r.screening_date = d1.full_date
LEFT JOIN DIM_DATE d2 ON r.enrollment_date = d2.full_date
LEFT JOIN DIM_DATE d3 ON r.randomization_date = d3.full_date;


select * from FACT_TRIAL_EVENTS;

-- 5. Fact: Clinical Metrics
CREATE TABLE IF NOT EXISTS FACT_CLINICAL_METRICS (
  metric_key           INT AUTO_INCREMENT PRIMARY KEY,
  patient_key          INT,
  date_key             INT,
  weight_kg            DECIMAL(6,2),          -- Increased precision to hold larger values
  height_cm            DECIMAL(5,2),
  bmi                  DECIMAL(5,2),
  systolic_bp          DECIMAL(5,1),
  diastolic_bp         DECIMAL(5,1),
  hemoglobin_gdl       DECIMAL(4,2),
  creatinine_mgdl      DECIMAL(5,3),          -- Increased precision for creatinine
  glucose_mgdl         DECIMAL(5,1),
  visit_completion_rate DECIMAL(5,3),          -- Increased precision
  missed_visits        INT,
  medication_adherence DECIMAL(5,3),           -- Increased precision
  data_quality_score   DECIMAL(5,2),
  FOREIGN KEY (patient_key) REFERENCES DIM_PATIENTS(patient_key),  -- Corrected table name to DIM_PATIENT
  FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key)
) ENGINE=InnoDB;

INSERT INTO FACT_CLINICAL_METRICS (
  patient_key,
  date_key,
  weight_kg, height_cm, bmi,
  systolic_bp, diastolic_bp,
  hemoglobin_gdl, creatinine_mgdl, glucose_mgdl,
  visit_completion_rate, missed_visits,
  medication_adherence, data_quality_score
)
SELECT
  p.patient_key,
  d.date_key,
  CAST(r.weight_kg AS DECIMAL(6,2)),
  CAST(r.height_cm AS DECIMAL(5,2)),
  CAST(r.bmi AS DECIMAL(5,2)),
  CAST(r.systolic_bp AS DECIMAL(5,1)),
  CAST(r.diastolic_bp AS DECIMAL(5,1)),
  CAST(r.hemoglobin_gdl AS DECIMAL(4,2)),
  CAST(r.creatinine_mgdl AS DECIMAL(5,3)),
  CAST(r.glucose_mgdl AS DECIMAL(5,1)),
  CAST(r.visit_completion_rate AS DECIMAL(5,3)),
  r.missed_visits,
  CAST(r.medication_adherence AS DECIMAL(5,3)),
  CAST(r.data_quality_score AS DECIMAL(5,2))
FROM raw_dataset r
JOIN DIM_PATIENTS p ON r.patient_id = p.patient_id
LEFT JOIN DIM_DATE d ON r.screening_date = d.full_date
WHERE r.screening_date IS NOT NULL;

select * from FACT_CLINICAL_METRICS;