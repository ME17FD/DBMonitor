# Database Performance Monitoring System

A comprehensive database monitoring tool that generates professional PDF reports with visual charts, tables, and executive summaries.

## Features

### 📊 Professional PDF Reports
- **Executive Summary**: Key metrics dashboard with status indicators
- **Visual Charts**: CPU/RAM usage, cache hit ratios, and storage usage
- **Data Tables**: Long-running queries, frequent queries, and replication status
- **Professional Design**: Corporate-grade styling with color-coded status indicators

### 📈 Visual Analytics
- **CPU/RAM Usage Charts**: Bar charts with warning/critical thresholds
- **Cache Hit Ratio**: Pie charts with performance status indicators
- **Storage Usage**: Bar charts showing database sizes
- **Status Indicators**: Color-coded alerts (🟢 Good, 🟡 Warning, 🔴 Critical)

### 📋 Data Collection
- **Query Analysis**: Long-running queries and most frequent queries with full query text
- **System Monitoring**: Both system and PostgreSQL server CPU/RAM metrics
- **Storage Analysis**: Per-database, per-table, and per-index storage breakdown
- **Cache Performance**: Total and per-table cache hit ratios with index vs heap analysis
- **Replication Monitoring**: Comprehensive replication lag and status tracking
- **Time Formatting**: Automatic conversion to appropriate time units (ms/s)
- **Storage Formatting**: Automatic conversion to appropriate storage units (KB/MB/GB)

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your database connection in `config/db_config.yaml`:
```yaml
database:
  host: "localhost"
  port: 5432
  database: "your_database"
  user: "your_username"
  password: "your_password"
```

## Usage

Run the monitoring system:
```bash
python main.py
```

This will generate a professional PDF report named `db_monitoring_report.pdf` with:
- Executive summary with key metrics
- Visual charts for system performance
- Detailed tables for database queries
- Replication status information

## Report Structure

### 1. Executive Summary
- CPU and RAM usage with status indicators
- Cache hit ratio performance
- Number of slow queries
- Overall system health status

### 2. System Performance
- **System Metrics**: CPU/RAM usage with PostgreSQL server configuration
- **Cache Analysis**: Total cache hit ratio and index vs heap usage ratios
- **Storage Overview**: Database and table storage usage charts

### 3. Database Performance
- **Long Running Queries**: Full query text with formatted execution times
- **Most Frequent Queries**: Complete queries with call counts and average times
- **Replication Status**: Current replication lag and connection information

### 4. Detailed Storage Analysis
- **Database Storage**: Per-database storage breakdown
- **Table Storage**: Per-table storage with index size separation
- **Index Storage**: Per-index storage usage analysis

### 5. Detailed Cache Analysis
- **Per-Table Cache**: Cache hit ratios for individual tables
- **Index vs Heap**: Analysis of index usage compared to heap access

## Professional Design Features

- **Color Scheme**: Professional blue/gray palette with status colors
- **Typography**: Clean, readable fonts with proper hierarchy
- **Layout**: Multi-page layout with proper spacing and margins
- **Charts**: High-resolution matplotlib charts with professional styling
- **Tables**: Formatted tables with alternating row colors and borders
- **Status Indicators**: Visual status indicators for quick assessment

## Dependencies

- `reportlab`: PDF generation
- `matplotlib`: Chart creation
- `seaborn`: Professional chart styling
- `pandas`: Data manipulation
- `numpy`: Numerical operations
- `psutil`: System monitoring
- `psycopg2`: PostgreSQL connectivity
- `pyyaml`: Configuration management

## Customization

The PDF generator can be customized by modifying the `ProfessionalPDFGenerator` class in `utils/pdf_generator.py`:

- **Colors**: Modify the color scheme in `_create_custom_styles()`
- **Charts**: Customize chart appearance in chart creation methods
- **Layout**: Adjust spacing, margins, and page breaks
- **Content**: Add or remove sections as needed

## Requirements

- Python 3.7+
- PostgreSQL database with `pg_stat_statements` extension enabled
- Sufficient permissions to query system statistics

## License

This project is designed for professional database monitoring and reporting.
