# Exchange Rate ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline that fetches daily currency exchange rates, cleans and validates the data, stores it in Supabase, and provides an interactive dashboard for visualization.

## 🎯 Features

- **ETL Framework**: Clear separation of Extract, Transform, and Load layers
- **Data Quality**: Comprehensive validation and cleaning operations
- **Error Handling**: Retry mechanisms, graceful degradation, and comprehensive logging
- **Configuration Management**: Environment-based configuration
- **Idempotency**: Safe re-runs with upsert operations
- **Observability**: Structured logging and metrics collection
- **Interactive Dashboard**: Real-time visualization with Streamlit
- **Testing**: Unit tests for all components

## 📋 Prerequisites

- Python 3.8+
- Supabase account (optional - dashboard works with dummy data)
- ExchangeRate.host API key (provided)

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or navigate to the project directory
cd "Exchange Rate Data Pipeline"

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Create a `.env` file in the project root (or copy from `.env.example`):

```env
# ExchangeRate.host API Configuration
EXCHANGERATE_API_KEY=cbf4d09c53b4d9a283362efa1895d665
EXCHANGERATE_BASE_URL=https://api.exchangerate.host

# Supabase Configuration (optional)
SUPABASE_URL=your_supabase_url_here
SUPABASE_KEY=your_supabase_key_here

# Pipeline Configuration
LOG_LEVEL=INFO
BATCH_SIZE=100
```

### 3. Set Up Supabase (Optional)

If you want to store data in Supabase, create the following table:

```sql
CREATE TABLE exchange_rates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    currency_pair VARCHAR(10) NOT NULL,
    rate DECIMAL(10, 4) NOT NULL,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(date, currency_pair)
);
```

### 4. Run the Pipeline

```bash
python run_pipeline.py
```

### 5. Launch the Dashboard

```bash
streamlit run src/dashboard/app.py
```

The dashboard will automatically use dummy data if Supabase is not configured.

## 📁 Project Structure

```
exchange-rate-pipeline/
├── src/
│   ├── extract/              # Extract layer
│   │   └── api_client.py     # ExchangeRate.host API client
│   ├── transform/            # Transform layer
│   │   ├── data_cleaner.py   # Data cleaning operations
│   │   └── data_validator.py # Data validation and quality checks
│   ├── load/                 # Load layer
│   │   └── supabase_loader.py # Supabase data loader
│   ├── pipeline/             # ETL orchestration
│   │   └── etl_pipeline.py   # Main ETL orchestrator
│   ├── utils/                # Utilities
│   │   ├── logger.py         # Logging configuration
│   │   └── config.py         # Configuration management
│   └── dashboard/            # Dashboard
│       ├── app.py            # Streamlit dashboard
│       └── data_generator.py # Dummy data generator
├── tests/                    # Unit tests
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
├── README.md                 # This file
└── run_pipeline.py           # Pipeline entry point
```

## 🏗️ Architecture

### ETL Pipeline Flow

1. **Extract**: Fetches exchange rates from ExchangeRate.host API
   - Supports USD, EUR, GBP → GHS
   - Includes retry logic and error handling
   - Returns structured data with timestamps

2. **Transform**: Cleans and validates the data
   - Removes duplicates
   - Handles missing values
   - Standardizes formats
   - Validates schema and business rules
   - Calculates data quality metrics

3. **Load**: Stores data in Supabase
   - Batch processing
   - Upsert operations (handles duplicates)
   - Error handling and rollback capabilities

### Data Engineering Concepts Showcased

1. **ETL Framework**: Modular design with clear separation of concerns
2. **Data Quality**: Multi-level validation (schema + business rules)
3. **Error Handling**: Retry mechanisms, graceful degradation
4. **Configuration Management**: Environment-based config
5. **Data Modeling**: Proper database schema design
6. **Idempotency**: Safe re-runs with upsert logic
7. **Observability**: Structured logging and metrics
8. **Testing**: Comprehensive unit test coverage

## 📊 Dashboard Features

- **Time Series Visualization**: Interactive charts showing exchange rate trends
- **Currency Comparison**: Side-by-side comparison of different currencies
- **Data Table**: Filterable and sortable data table
- **Statistics**: Summary statistics and metrics
- **Dummy Data Mode**: Works without Supabase configuration
- **Real-time Data**: Automatically switches to Supabase when configured

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_extract.py
```

## 📝 Usage Examples

### Running the Pipeline Programmatically

```python
from src.pipeline.etl_pipeline import ETLPipeline

# Initialize and run pipeline
pipeline = ETLPipeline()
results = pipeline.run()

# Check results
if results['success']:
    print(f"Processed {results['records_processed']} records")
    print(f"Quality: {results['quality_metrics']['completeness']:.2%}")
else:
    print(f"Error: {results['error']}")
```

### Fetching Rates Manually

```python
from src.extract.api_client import ExchangeRateAPIClient

client = ExchangeRateAPIClient()
rates = client.fetch_latest_rates(['USD', 'EUR'], 'GHS')
print(rates)
```

### Loading Data Manually

```python
from src.load.supabase_loader import SupabaseLoader

loader = SupabaseLoader()
result = loader.load_batch(data)
print(f"Loaded: {result['success_count']} records")
```

## 🔧 Configuration Options

- `EXCHANGERATE_API_KEY`: Your ExchangeRate.host API key
- `EXCHANGERATE_BASE_URL`: API base URL (default: https://api.exchangerate.host)
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase API key
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `BATCH_SIZE`: Number of records per batch when loading (default: 100)

## 📈 Monitoring and Logging

The pipeline uses structured logging with the following format:

```
[YYYY-MM-DD HH:MM:SS] [LEVEL] [MODULE] MESSAGE
```

ETL stages are logged with context:
```
[EXTRACT] Starting data extraction
[TRANSFORM] Data transformation completed | records=3 | completeness=100.00%
[LOAD] Data loading completed | success_count=3 | error_count=0
```

## 🐛 Troubleshooting

### API Errors
- Verify your API key is correct
- Check network connectivity
- Review API rate limits

### Supabase Connection Issues
- Verify SUPABASE_URL and SUPABASE_KEY in `.env`
- Check Supabase project status
- Ensure table schema matches expected structure

### Dashboard Not Loading
- Ensure Streamlit is installed: `pip install streamlit`
- Check that port 8501 is available
- Review console for error messages

## 🤝 Contributing

This is a demonstration project showcasing ETL and data engineering concepts. Feel free to extend it with:
- Additional data sources
- More sophisticated transformations
- Advanced visualizations
- Scheduling/automation (Airflow, cron, etc.)
- Data quality monitoring
- Alerting mechanisms

## 📄 License

This project is for demonstration purposes.

## 🙏 Acknowledgments

- ExchangeRate.host for providing the API
- Supabase for the database platform
- Streamlit for the dashboard framework

