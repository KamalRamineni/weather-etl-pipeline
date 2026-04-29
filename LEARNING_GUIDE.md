# 📖 Data Engineering Concepts - Learning Guide

This guide explains every data engineering concept you'll encounter while building this pipeline.

---

## 🎯 Core ETL Concepts

### What is ETL?

**ETL = Extract, Transform, Load**

It's the process of:
1. **Extract**: Getting data from source systems (APIs, databases, files)
2. **Transform**: Cleaning, restructuring, and preparing data
3. **Load**: Writing data to destination (data warehouse, database)

**Why do we need ETL?**
- Source data is often messy, inconsistent, or in wrong format
- Different systems store data differently
- Analytics tools need clean, structured data

**Real-world example:**
Imagine you're collecting weather from 10 different websites. Each shows temperature differently (°F vs °C), different formats (JSON vs XML), different update frequencies. ETL standardizes everything into one clean database.

---

## 📊 Data Modeling Concepts

### Fact vs Dimension Tables

Our pipeline uses **dimensional modeling** (star schema):

```
┌─────────────┐
│   CITIES    │ ← Dimension table (WHO/WHERE)
│ (Dimension) │
├─────────────┤
│ city_id (PK)│
│ city_name   │
│ country     │
│ lat/lon     │
└─────┬───────┘
      │
      │ Foreign Key
      ↓
┌─────────────────┐
│ WEATHER_DATA    │ ← Fact table (MEASUREMENTS)
│    (Fact)       │
├─────────────────┤
│ weather_id (PK) │
│ city_id (FK)    │───┐
│ recorded_at     │   │ Links to dimension
│ temperature     │←──┘
│ humidity        │
│ pressure        │
└─────────────────┘
```

**Dimension Tables** - The "WHO, WHAT, WHERE, WHEN"
- Store descriptive attributes
- Change slowly (cities don't move)
- Referenced by facts
- Example: Cities, Products, Customers

**Fact Tables** - The "MEASUREMENTS"
- Store numeric metrics
- Change frequently (weather updates hourly)
- Contain foreign keys to dimensions
- Example: Sales, Weather readings, Web visits

**Why separate them?**
1. **Avoid duplication**: Store city info once, not in every weather record
2. **Performance**: Faster queries (smaller fact table)
3. **Flexibility**: Easy to add new cities without changing weather data

---

## 🔄 Data Pipeline Patterns

### Batch vs Streaming

Our pipeline is **batch processing**:
- Runs at scheduled intervals (hourly, daily)
- Processes data in groups
- Good for analytics, reporting

**Streaming** would be:
- Processes data in real-time as it arrives
- Used for: fraud detection, live dashboards
- Tools: Kafka, Kinesis, Flink

**When to use batch (like our pipeline):**
- Historical analysis
- Data doesn't need to be real-time
- Lower cost, simpler to maintain

**When to use streaming:**
- Real-time alerts needed
- Fraud detection
- Live monitoring

---

## 🗃️ Database Concepts

### ACID Transactions

ACID ensures database reliability:

**A - Atomicity**: All or nothing
```python
# Either BOTH operations succeed, or BOTH fail
1. Insert city
2. Insert weather data
# If #2 fails, #1 is rolled back
```

**C - Consistency**: Database rules are enforced
```sql
-- Foreign key constraint ensures data integrity
city_id in weather_data MUST exist in cities table
```

**I - Isolation**: Concurrent operations don't interfere
```python
# Two pipelines running simultaneously won't corrupt data
Pipeline A: Loading New York weather
Pipeline B: Loading London weather
# They don't interfere with each other
```

**D - Durability**: Committed data persists
```python
# Once commit() succeeds, data survives crashes/power loss
conn.commit()  # Data is now permanent
```

### Primary Keys vs Foreign Keys

**Primary Key (PK)**: Unique identifier for a row
```sql
CREATE TABLE cities (
    city_id SERIAL PRIMARY KEY,  -- Unique ID for each city
    city_name VARCHAR(100)
);
```

**Foreign Key (FK)**: References primary key in another table
```sql
CREATE TABLE weather_data (
    weather_id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(city_id),  -- FK to cities
    temperature DECIMAL
);
```

**Why use them?**
- **Data integrity**: Can't insert weather for non-existent city
- **Relationships**: Links tables together
- **Performance**: Enables efficient joins

### Indexes

Indexes make queries faster (like a book index):

```sql
-- Without index: scans ALL rows (slow)
SELECT * FROM weather_data WHERE city_id = 5;

-- With index: jumps directly to matching rows (fast)
CREATE INDEX idx_weather_city ON weather_data(city_id);
```

**Trade-offs:**
- ✅ Faster queries (SELECT)
- ❌ Slower inserts (must update index)
- ❌ Uses disk space

**When to create indexes:**
- Columns used in WHERE clauses
- Columns used in JOINs
- Columns used in ORDER BY

**Our indexes:**
```sql
idx_weather_city      → Fast: WHERE city_id = X
idx_weather_time      → Fast: WHERE recorded_at > '2024-01-01'
idx_weather_city_time → Fast: WHERE city_id = X AND recorded_at > Y
```

---

## 🔧 Python & SQL Techniques

### Upsert (INSERT ... ON CONFLICT)

**Problem**: What if we run the pipeline twice?
- Don't want duplicate data
- But DO want to update if data changed

**Solution**: Upsert = INSERT + UPDATE

```sql
INSERT INTO weather_data (city_id, recorded_at, temperature)
VALUES (1, '2024-01-15 12:00', 20.5)
ON CONFLICT (city_id, recorded_at)  -- If same city + time exists
DO UPDATE SET                       -- Update instead of insert
    temperature = EXCLUDED.temperature;
```

**Makes pipeline idempotent** = safe to run multiple times

### Pandas DataFrames

**What is it?**
Think of it as Excel in Python - a table with rows and columns.

**Why use pandas?**
```python
# Without pandas (manual):
for record in records:
    if record['temperature'] is None:
        record['temperature'] = 0
    record['temperature'] = float(record['temperature'])

# With pandas (automatic):
df['temperature'] = df['temperature'].fillna(0).astype(float)
```

**Key operations:**
```python
df.drop_duplicates()           # Remove duplicate rows
df.fillna(0)                   # Replace missing values
df.to_csv('data.csv')          # Export to CSV
df.to_sql('table', conn)       # Load to database
```

### SQLAlchemy vs Raw SQL

**Raw SQL:**
```python
cursor.execute("SELECT * FROM cities WHERE city_name = 'London'")
```

**SQLAlchemy:**
```python
from sqlalchemy import text
conn.execute(text("SELECT * FROM cities WHERE city_name = :name"), 
             {'name': 'London'})
```

**Benefits of SQLAlchemy:**
1. **SQL injection protection**: Parameterized queries
2. **Database abstraction**: Works with PostgreSQL, MySQL, SQLite
3. **Connection pooling**: Reuses connections for better performance

---

## 🛡️ Data Quality & Best Practices

### Data Validation

**Schema Validation**: Ensure correct data types
```python
df['temperature'] = df['temperature'].astype(float)  # Must be number
df['recorded_at'] = pd.to_datetime(df['recorded_at'])  # Must be date
```

**Range Validation**: Check for realistic values
```python
if (df['temperature'] < -100).any():  # Impossible temperature
    print("Warning: Invalid temperature detected")
```

**Null Handling**: Decide what to do with missing data
```python
# Strategy 1: Drop rows with missing critical data
df = df.dropna(subset=['city_name', 'temperature'])

# Strategy 2: Fill with defaults
df['rain_1h'] = df['rain_1h'].fillna(0.0)

# Strategy 3: Fill with statistical values
df['temperature'] = df['temperature'].fillna(df['temperature'].mean())
```

### Error Handling & Retries

**Why retry?**
APIs can fail temporarily (network glitches, rate limits)

**Our retry logic:**
```python
for attempt in range(3):  # Try 3 times
    try:
        response = requests.get(url)
        return response.json()
    except:
        if attempt < 2:
            wait = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            time.sleep(wait)
```

**Exponential backoff** prevents overwhelming the server.

### Logging vs Printing

**Current approach (print):**
```python
print("✅ Data loaded successfully")
```

**Professional approach (logging):**
```python
import logging
logging.info("Data loaded successfully")
logging.error("Failed to connect to API", exc_info=True)
```

**Why logging is better:**
- Can write to files
- Different severity levels (DEBUG, INFO, WARNING, ERROR)
- Includes timestamps automatically
- Can disable in production

---

## 📈 Performance Optimization

### Batch Loading vs Individual Inserts

**Bad (slow):**
```python
for record in records:
    conn.execute("INSERT INTO table VALUES (?)", record)
    conn.commit()  # Commit after EACH insert
```

**Good (fast):**
```python
conn.executemany("INSERT INTO table VALUES (?)", records)
conn.commit()  # One commit for ALL inserts
```

**Why faster?**
- Reduces database round trips
- Single transaction vs many transactions
- Network overhead reduced

### Connection Pooling

**Without pooling:**
```python
# Each operation opens new connection (slow)
conn = connect_to_db()
conn.execute(query)
conn.close()
```

**With pooling (SQLAlchemy):**
```python
# Reuses existing connections
engine = create_engine(url, pool_size=5)
# Maintains 5 open connections, reuses them
```

**Benefits:**
- Faster (no connection overhead)
- Better resource usage
- Handles concurrent requests

---

## 🔐 Security Best Practices

### Environment Variables

**Bad (hardcoded secrets):**
```python
api_key = "abc123xyz"  # Committed to Git!
password = "secret123"  # Visible to everyone!
```

**Good (.env file):**
```python
# .env (in .gitignore)
API_KEY=abc123xyz
DB_PASSWORD=secret123

# Code
api_key = os.getenv('API_KEY')
```

**Why?**
- Secrets not in version control
- Different values per environment (dev/staging/prod)
- Easy to rotate credentials

### SQL Injection Prevention

**Vulnerable:**
```python
city = input("Enter city: ")
query = f"SELECT * FROM cities WHERE name = '{city}'"
# User enters: "London'; DROP TABLE cities; --"
# 😱 Your entire table gets deleted!
```

**Safe (parameterized queries):**
```python
query = text("SELECT * FROM cities WHERE name = :city")
conn.execute(query, {'city': city})
# SQLAlchemy escapes special characters automatically
```

---

## 🎓 Advanced Concepts (For Later)

### Slowly Changing Dimensions (SCD)

What if city data changes? (e.g., timezone, population)

**Type 1**: Overwrite (lose history)
**Type 2**: Add new row (keep history)
**Type 3**: Add new column

### Data Lineage

Track where data came from:
```python
data['extraction_timestamp'] = datetime.now()
data['source'] = 'openweathermap_api'
```

### Incremental vs Full Refresh

**Full Refresh**: Load all data every time
```python
# Our current approach
DELETE FROM weather_data;
INSERT INTO weather_data SELECT * FROM staging;
```

**Incremental**: Only load new/changed data
```python
# Advanced: Only insert records newer than last load
WHERE recorded_at > (SELECT MAX(recorded_at) FROM weather_data)
```

---

## 🚀 Next-Level Tools & Concepts

### Apache Airflow
- Workflow orchestration
- DAG (Directed Acyclic Graph) for dependencies
- Scheduling, monitoring, alerting

### dbt (data build tool)
- SQL-based transformations
- Version control for SQL
- Testing and documentation

### Great Expectations
- Data quality testing
- Automated validation
- Documentation generation

### Docker
- Containerization
- Consistent environments
- Easy deployment

---

## 💡 Key Takeaways

1. **ETL is a pattern**, not a tool - you can implement it many ways
2. **Data quality matters** - validate, clean, handle nulls
3. **Idempotency is crucial** - pipelines should be safe to re-run
4. **Error handling is essential** - networks fail, APIs have limits
5. **Think in batches** - process data in groups for efficiency
6. **Security first** - never hardcode credentials
7. **Monitor everything** - log, track, measure

---

## 📚 Learning Resources by Topic

### SQL Fundamentals
- [SQLBolt](https://sqlbolt.com/) - Interactive tutorial
- [Mode SQL Tutorial](https://mode.com/sql-tutorial/)

### Python for Data
- [Real Python](https://realpython.com/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

### Data Engineering
- Book: "Fundamentals of Data Engineering" by Joe Reis
- [DataTalks.Club](https://datatalks.club/) - Free courses

### System Design
- [System Design Primer](https://github.com/donnemartin/system-design-primer)
- Practice: Draw architecture diagrams of your pipeline

---

## 🎯 Practice Exercises

1. **Add data validation**: Check if temperature is within reasonable range
2. **Implement logging**: Replace print statements with proper logging
3. **Add error notifications**: Send email when pipeline fails
4. **Create incremental loading**: Only load new records since last run
5. **Write tests**: Unit tests for transformer functions
6. **Optimize queries**: Add indexes and measure performance improvement
7. **Add monitoring**: Track pipeline execution time, record counts

---

This guide grows with you - refer back as you encounter new concepts!
