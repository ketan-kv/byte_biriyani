CREATE TABLE IF NOT EXISTS geological_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    survey_date DATE,
    location_name TEXT,
    latitude REAL,
    longitude REAL,
    depth_m REAL,
    mineral_type TEXT,
    grade_value REAL,
    grade_unit TEXT,
    rock_type TEXT,
    zone_id TEXT,
    confidence REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS extraction_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    log_date DATE NOT NULL,
    shift TEXT,
    zone_id TEXT,
    equipment_id TEXT,
    mineral_type TEXT,
    yield_tonnes REAL,
    ore_processed_t REAL,
    efficiency_pct REAL,
    downtime_hours REAL,
    operator_id TEXT,
    notes TEXT,
    source_file TEXT
);

CREATE TABLE IF NOT EXISTS incident_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_date DATE NOT NULL,
    incident_time TIME,
    zone_id TEXT,
    equipment_id TEXT,
    incident_type TEXT,
    severity TEXT,
    description TEXT,
    root_cause TEXT,
    corrective_action TEXT,
    reported_by TEXT,
    resolved BOOLEAN DEFAULT 0,
    resolution_date DATE,
    source_file TEXT
);
