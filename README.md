# Exchange Rate ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline that fetches daily currency exchange rates, cleans and validates the data, stores it in Supabase, and provides an interactive dashboard for visualization.


Quick Start

### 1. Installation

```bash
# Clone or navigate to the project directory
cd "Exchange Rate Data Pipeline"

# Install dependencies
pip install -r requirements.txt
```


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

 Architecture

### ETL Pipeline Flow

