# Weather ETL Pipeline Project

## What You'll Build
An automated ETL pipeline that:
- **Extracts** weather data from OpenWeatherMap API
- **Transforms** raw JSON into clean, structured data
- **Loads** data into PostgreSQL database
- **Schedules** automatic runs every hour

## What You'll Learn
- API interaction and authentication
- Data extraction and error handling
- Data transformation with pandas
- Database operations (CREATE, INSERT, SELECT)
- SQL queries and data analysis
- Environment variables and secrets management
- Code organization and best practices

---

## Prerequisites Setup

### 1. Install Python (if not already installed)
```bash
# Check if Python is installed
python --version  # Should be 3.8+

# If not installed, download from python.org
```

### 2. Install PostgreSQL
**Windows**: Download from postgresql.org
**Mac**: `brew install postgresql`
**Linux**: `sudo apt-get install postgresql`

### 3. Create Project Directory
```bash
mkdir weather-etl-pipeline
cd weather-etl-pipeline
```

### 4. Set Up Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate
```

### 5. Install Required Packages
```bash
pip install pandas requests sqlalchemy psycopg2-binary python-dotenv schedule
```

---

## Project Structure
```
weather-etl-pipeline/
├── config/
│   └── database.py          # Database connection setup
├── extractors/
│   └── weather_api.py       # API data extraction
├── transformers/
│   └── weather_transform.py # Data cleaning & transformation
├── loaders/
│   └── database_loader.py   # Load data to database
├── sql/
│   └── create_tables.sql    # Database schema
├── main.py                  # Orchestration script
├── .env                     # Environment variables (API keys)
├── .gitignore              # Files to ignore in Git
└── requirements.txt         # Python dependencies
```

---

## Get Your API Key

1. Go to https://openweathermap.org/api
2. Sign up for free account
3. Generate API key (takes ~10 minutes to activate)
4. Save it - we'll use it next!

---

## Next Steps
Once you have:
- ✅ Python installed
- ✅ PostgreSQL installed
- ✅ Virtual environment activated
- ✅ Packages installed
- ✅ API key obtained

We'll start building the pipeline!
