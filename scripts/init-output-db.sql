-- Output Database Schema
-- This contains the aggregated pet activity enjoyment data for Grafana dashboards

-- Pet Activity Enjoyment Data Product - Main aggregated table
CREATE TABLE IF NOT EXISTS pet_activity_enjoyment (
    id SERIAL PRIMARY KEY,
    collar_id VARCHAR(50) NOT NULL,
    pet_name VARCHAR(100),
    species VARCHAR(50),
    breed VARCHAR(100),
    owner_name VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    activity_type VARCHAR(50) NOT NULL,
    avg_enjoyment_score DECIMAL(5,2),
    total_events INTEGER,
    total_duration_minutes INTEGER,
    measurement_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(collar_id, activity_type, measurement_date)
);

CREATE INDEX idx_activity_date ON pet_activity_enjoyment(measurement_date);
CREATE INDEX idx_activity_collar ON pet_activity_enjoyment(collar_id);
CREATE INDEX idx_activity_type ON pet_activity_enjoyment(activity_type);

-- Daily summary by species
CREATE TABLE IF NOT EXISTS daily_activity_by_species (
    id SERIAL PRIMARY KEY,
    measurement_date DATE NOT NULL,
    species VARCHAR(50) NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    pet_count INTEGER,
    avg_enjoyment_score DECIMAL(5,2),
    total_events INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(measurement_date, species, activity_type)
);

-- Daily summary by city
CREATE TABLE IF NOT EXISTS daily_activity_by_city (
    id SERIAL PRIMARY KEY,
    measurement_date DATE NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    pet_count INTEGER,
    avg_enjoyment_score DECIMAL(5,2),
    total_events INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(measurement_date, city, state, activity_type)
);

-- Top activities ranking
CREATE TABLE IF NOT EXISTS top_activities (
    id SERIAL PRIMARY KEY,
    measurement_date DATE NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    rank INTEGER NOT NULL,
    avg_enjoyment_score DECIMAL(5,2),
    participation_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(measurement_date, activity_type)
);

-- Pipeline run metadata
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) UNIQUE NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'running',
    events_processed INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    error_message TEXT
);

-- Create views for Grafana
CREATE VIEW v_latest_activity_summary AS
SELECT
    activity_type,
    COUNT(DISTINCT collar_id) as unique_pets,
    ROUND(AVG(avg_enjoyment_score), 2) as overall_enjoyment,
    SUM(total_events) as total_events,
    SUM(total_duration_minutes) as total_minutes
FROM pet_activity_enjoyment
WHERE measurement_date = (SELECT MAX(measurement_date) FROM pet_activity_enjoyment)
GROUP BY activity_type
ORDER BY overall_enjoyment DESC;

CREATE VIEW v_species_activity_trends AS
SELECT
    measurement_date,
    species,
    activity_type,
    avg_enjoyment_score,
    pet_count
FROM daily_activity_by_species
ORDER BY measurement_date DESC, avg_enjoyment_score DESC;
