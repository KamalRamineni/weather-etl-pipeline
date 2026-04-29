-- Create database (run this manually first)
-- CREATE DATABASE weather_db;

-- Drop existing tables if they exist (for development)
DROP VIEW IF EXISTS weather_current;
DROP TABLE IF EXISTS weather_data;
DROP TABLE IF EXISTS cities;

-- Cities dimension table
-- This stores static information about cities we're tracking
CREATE TABLE cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE,
    country_code VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather fact table
-- This stores time-series weather measurements
CREATE TABLE weather_data (
    weather_id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(city_id),
    
    -- Timestamp information
    recorded_at TIMESTAMP NOT NULL,
    data_fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Temperature data (Celsius)
    temperature DECIMAL(5, 2),
    feels_like DECIMAL(5, 2),
    temp_min DECIMAL(5, 2),
    temp_max DECIMAL(5, 2),
    
    -- Atmospheric conditions
    pressure INTEGER,  -- hPa
    humidity INTEGER,  -- percentage
    
    -- Weather conditions
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    
    -- Wind
    wind_speed DECIMAL(5, 2),  -- m/s
    wind_deg INTEGER,           -- degrees
    
    -- Clouds & visibility
    cloudiness INTEGER,  -- percentage
    visibility INTEGER,  -- meters
    
    -- Rain & snow (optional - can be NULL)
    rain_1h DECIMAL(5, 2),
    snow_1h DECIMAL(5, 2),
    
    -- Create index for faster queries
    CONSTRAINT unique_city_time UNIQUE(city_id, recorded_at)
);

-- Create indexes for common query patterns
CREATE INDEX idx_weather_city ON weather_data(city_id);
CREATE INDEX idx_weather_time ON weather_data(recorded_at);
CREATE INDEX idx_weather_city_time ON weather_data(city_id, recorded_at DESC);

-- Create a view for easy querying with city names
CREATE VIEW weather_current AS
SELECT 
    c.city_name,
    c.country_code,
    w.recorded_at,
    w.temperature,
    w.feels_like,
    w.humidity,
    w.weather_description,
    w.wind_speed
FROM weather_data w
JOIN cities c ON w.city_id = c.city_id
ORDER BY w.recorded_at DESC;

-- Example queries you can run later:

-- Get latest weather for all cities
-- SELECT * FROM weather_current LIMIT 10;

-- Get temperature trends for a city
-- SELECT 
--     DATE(recorded_at) as date,
--     AVG(temperature) as avg_temp,
--     MAX(temperature) as max_temp,
--     MIN(temperature) as min_temp
-- FROM weather_data w
-- JOIN cities c ON w.city_id = c.city_id
-- WHERE c.city_name = 'London'
-- GROUP BY DATE(recorded_at)
-- ORDER BY date DESC;
