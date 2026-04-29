# 🚀 Getting Started Guide - Weather ETL Pipeline

This guide will walk you through running your first ETL pipeline, step by step!

## 📋 Pre-Flight Checklist

Before we start, make sure you have:
- [ ] Python 3.8+ installed
- [ ] PostgreSQL installed and running
- [ ] OpenWeatherMap API key (free tier)
- [ ] Basic command line familiarity

---

## Step 1: Set Up Your Environment

### 1.1 Navigate to Project Directory
```bash
cd weather-etl-pipeline
```

### 1.2 Create and Activate Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate

# On Mac/Linux:
source venv/bin/activate

# You should see (venv) at the start of your command prompt
```

### 1.3 Install Required Packages
```bash
pip install -r requirements.txt

# You should see installation messages for:
# - pandas
# - requests
# - sqlalchemy
# - psycopg2-binary
# - python-dotenv
```

---

## Step 2: Configure Database

### 2.1 Start PostgreSQL
Make sure PostgreSQL is running on your system.

**To check if it's running:**
```bash
# On Mac/Linux:
sudo service postgresql status

# On Windows:
# Check Services app for "postgresql" service
```

### 2.2 Create Database
Open PostgreSQL command line (psql) or use a GUI tool like pgAdmin.

```sql
-- Create the database
CREATE DATABASE weather_db;

-- Verify it was created
\l  -- (in psql) shows list of databases
```

### 2.3 Set Up Database Password
If you haven't set a password for postgres user:

```sql
-- In psql:
ALTER USER postgres PASSWORD 'your_password_here';
```

---

## Step 3: Configure Environment Variables

### 3.1 Create .env File
Copy the example file:
```bash
cp .env.example .env
```

### 3.2 Edit .env File
Open `.env` in your text editor and fill in your details:

```env
# API Configuration
OPENWEATHER_API_KEY=YOUR_ACTUAL_API_KEY_HERE

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=your_password_here

# Cities to track (comma-separated)
CITIES=New York,London,Tokyo,Mumbai,Sydney
```

**Important:** 
- Replace `YOUR_ACTUAL_API_KEY_HERE` with your OpenWeatherMap API key
- Replace `your_password_here` with your PostgreSQL password
- You can add/remove cities from the CITIES list

---

## Step 4: Initialize Database Schema

Run the setup command to create tables:

```bash
python main.py --setup
```

**You should see:**
```
🔧 Setting up database schema...
✅ Successfully executed SQL file: sql/create_tables.sql
✅ Database schema created successfully

📋 Created tables:
  - cities
  - weather_data
  - weather_current
```

**What just happened?**
- Created `cities` table (stores city information)
- Created `weather_data` table (stores weather measurements)
- Created `weather_current` view (for easy querying)
- Created indexes for fast queries

---

## Step 5: Run Your First ETL Pipeline! 🎉

```bash
python main.py
```

**You'll see the pipeline execute in 4 phases:**

### Phase 1: EXTRACT
```
🔄 Starting extraction for 5 cities...
  Fetching data for New York... (attempt 1)
  ✅ Successfully extracted data for New York
  ...
✅ Extraction complete: 5/5 successful
```

### Phase 2: TRANSFORM
```
🔄 Transforming 5 records...
  Initial record count: 5
  ✅ Transformation complete: 5 records ready for loading

  📊 Data Quality Summary:
    Total records: 5
    Unique cities: 5
    Date range: ...
```

### Phase 3: LOAD
```
🔄 Loading cities into database...
  ✅ Loaded 5 cities

🔄 Loading weather data into database...
  ✅ Loaded 5 weather records
```

### Phase 4: VALIDATE
```
📊 Latest 5 records in database:
[Table showing your data]

📈 Database Statistics:
  Total cities tracked: 5
  Total weather records: 5
  Data spans: ...
```

### Final Summary
```
📊 PIPELINE EXECUTION SUMMARY
Start time:    2024-01-XX XX:XX:XX
End time:      2024-01-XX XX:XX:XX
Duration:      X.XX seconds

Records:
  Extracted:   5
  Transformed: 5
  Loaded:      5

✅ Pipeline completed successfully!
```

---

## Step 6: Verify Your Data

### 6.1 Query Database Directly

Open PostgreSQL command line:
```bash
psql -U postgres -d weather_db
```

Run some queries:

```sql
-- See all cities
SELECT * FROM cities;

-- See latest weather
SELECT * FROM weather_current LIMIT 10;

-- Get temperature statistics for a city
SELECT 
    city_name,
    AVG(temperature) as avg_temp,
    MAX(temperature) as max_temp,
    MIN(temperature) as min_temp
FROM weather_current
WHERE city_name = 'London'
GROUP BY city_name;

-- Count total records
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT city_name) as unique_cities
FROM weather_current;
```

### 6.2 Export Data to CSV (Optional)

Run the pipeline with export flag:
```bash
python main.py --export
```

This creates a CSV file you can open in Excel or Google Sheets!

---

## Step 7: Test Individual Components

Each component can be tested independently:

### Test Database Connection
```bash
python config/database.py
```

### Test API Extractor
```bash
python extractors/weather_api.py
```

### Test Transformer
```bash
python transformers/weather_transform.py
```

### Test Loader
```bash
python loaders/database_loader.py
```

---

## 🎓 What You Just Learned

Congratulations! You've built and run a complete ETL pipeline. Here's what you learned:

### **Technical Skills:**
1. ✅ **API Integration**: Authenticating and fetching data from REST APIs
2. ✅ **Data Extraction**: Handling API responses, retries, rate limiting
3. ✅ **Data Transformation**: Cleaning, flattening, type conversion with pandas
4. ✅ **Data Loading**: Database inserts, upserts, transaction management
5. ✅ **SQL**: Creating tables, indexes, views, foreign keys
6. ✅ **Python**: Object-oriented programming, error handling, logging
7. ✅ **Environment Management**: Virtual environments, environment variables

### **Data Engineering Concepts:**
1. ✅ **ETL Pattern**: Extract → Transform → Load workflow
2. ✅ **Data Modeling**: Dimensional modeling (fact & dimension tables)
3. ✅ **Data Quality**: Validation, missing value handling, type enforcement
4. ✅ **Idempotency**: Safe to run multiple times (upsert logic)
5. ✅ **Error Handling**: Graceful failure recovery
6. ✅ **Observability**: Logging, monitoring, statistics

---

## 🚀 Next Steps

Now that you have a working pipeline, here are next steps:

### Level 1: Enhance Current Pipeline
1. **Add More Cities**: Edit CITIES in .env
2. **Schedule Automation**: Set up cron job or Task Scheduler
3. **Add More Data Sources**: Weather forecast API, air quality, etc.
4. **Create Visualizations**: Connect to Tableau/PowerBI/Matplotlib

### Level 2: Add Advanced Features
1. **Data Quality Checks**: Implement Great Expectations
2. **Alerting**: Send email/Slack notifications on failures
3. **Incremental Loading**: Only load new data, not full refresh
4. **Historical Analysis**: Track weather trends over time

### Level 3: Move to Cloud
1. **Deploy to AWS**: Lambda + S3 + RDS
2. **Use Airflow**: Professional workflow orchestration
3. **Implement dbt**: Modern transformation framework
4. **Add Streaming**: Real-time data with Kafka/Kinesis

### Level 4: Portfolio Projects
1. **Similar patterns, different domains:**
   - Stock market data pipeline
   - Social media sentiment analysis
   - E-commerce sales analytics
   - Sports statistics aggregator

---

## 🐛 Troubleshooting

### "ModuleNotFoundError"
**Solution:** Make sure virtual environment is activated
```bash
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows
```

### "Invalid API key"
**Solution:** Check your .env file
- Verify OPENWEATHER_API_KEY is set correctly
- Make sure no extra spaces
- API keys take ~10 minutes to activate after creation

### "Database connection failed"
**Solution:** Check PostgreSQL
1. Is PostgreSQL running? `sudo service postgresql status`
2. Is the database created? `psql -l`
3. Is password correct in .env?
4. Is DB_HOST correct (usually 'localhost')?

### "No data extracted"
**Solution:** Check your internet connection and API limits
- Free tier: 60 calls/minute, 1,000 calls/day
- Check city names are spelled correctly

### "Transformation failed"
**Solution:** Check API response structure
- Run `python extractors/weather_api.py` to see raw data
- Verify JSON structure matches transformer expectations

---

## 📚 Additional Resources

### Learn More About:
- **SQL**: [SQLBolt](https://sqlbolt.com/), [Mode Analytics SQL Tutorial](https://mode.com/sql-tutorial/)
- **Python**: [Real Python](https://realpython.com/)
- **Pandas**: [10 Minutes to Pandas](https://pandas.pydata.org/docs/user_guide/10min.html)
- **Data Engineering**: "Fundamentals of Data Engineering" by Joe Reis

### Practice:
- Add error notifications via email
- Create a dashboard with Matplotlib
- Write SQL queries for temperature trends
- Optimize query performance with indexes

---

## 🎉 Congratulations!

You've successfully built and run your first production-grade ETL pipeline!

This is a portfolio-ready project that demonstrates:
- Real API integration
- Data transformation skills
- Database design and management
- Python best practices
- Error handling and monitoring

**Ready to build more?** Check out the main README for the full learning path!

---

## Need Help?

Common issues:
1. API key not working? Wait 10-15 minutes after generation
2. Database errors? Verify PostgreSQL is running
3. Import errors? Check virtual environment is activated
4. Data not loading? Run `python main.py --setup` first

**Pro tip:** Run each component test file individually to isolate issues!
