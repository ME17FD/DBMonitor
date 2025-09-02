from queries.long_queries import get_long_queries
from queries.frequent_queries import get_frequent_queries
from reports.cpu_ram import get_cpu_ram_usage
from reports.cache_hit import get_cache_hit_ratio
from reports.storage_usage import get_storage_usage
from logs.replication_delay import get_replication_delay
from utils.pdf_generator import ProfessionalPDFGenerator

def main():
    report_data = {}

    report_data["Long Queries"] = get_long_queries()
    report_data["Frequent Queries"] = get_frequent_queries()
    report_data["CPU/RAM Usage"] = get_cpu_ram_usage()
    report_data["Cache Hit Ratio"] = get_cache_hit_ratio()
    report_data["Storage Usage"] = get_storage_usage()
    report_data["Replication Delay"] = get_replication_delay()

    # Generate professional PDF report
    generator = ProfessionalPDFGenerator("db_monitoring_report.pdf")
    generator.generate_pdf(report_data)

if __name__ == "__main__":
    main()
