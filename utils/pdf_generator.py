from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from datetime import datetime
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import io
import yaml


# Set professional styling
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

class ProfessionalPDFGenerator:
    def __init__(self, filename="db_monitoring_report.pdf"):
        self.filename = filename
        self.styles = self._create_custom_styles()
        self.doc = SimpleDocTemplate(filename, pagesize=A4, 
                                   rightMargin=72, leftMargin=72, 
                                   topMargin=72, bottomMargin=18,
                                    title="Database Performance Monitoring Report",
                                        author="DBMONITOR")
        self.elements = []
        
    def _create_custom_styles(self):
        """Create custom professional styles"""
        styles = getSampleStyleSheet()
            
        # Title style
        styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=styles['Title'],
            fontSize=24,
            spaceAfter=30,
            alignment=1,  # Center
            textColor=colors.HexColor('#2c3e50'),
            fontName='Helvetica-Bold'
        ))

        # Section header style
        styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=12,
            spaceBefore=20,
            textColor=colors.HexColor('#34495e'),
            fontName='Helvetica-Bold',
            borderWidth=1,
            borderColor=colors.HexColor('#3498db'),
            borderPadding=8,
            backColor=colors.HexColor('#ecf0f1')
        ))

        # Subsection style
        styles.add(ParagraphStyle(
            name='Subsection',
            parent=styles['Heading3'],
            fontSize=14,
            spaceAfter=8,
            spaceBefore=12,
            textColor=colors.HexColor('#2c3e50'),
            fontName='Helvetica-Bold'
        ))

        # Metric style for key numbers
        styles.add(ParagraphStyle(
            name='Metric',
            parent=styles['Normal'],
            fontSize=12,
            textColor=colors.HexColor('#27ae60'),
            fontName='Helvetica-Bold',
            alignment=1
        ))

        # Warning style for alerts
        styles.add(ParagraphStyle(
            name='Warning',
            parent=styles['Normal'],
            fontSize=11,
            textColor=colors.HexColor('#e74c3c'),
            fontName='Helvetica-Bold',
            backColor=colors.HexColor('#fadbd8'),
            borderWidth=1,
            borderColor=colors.HexColor('#e74c3c'),
            borderPadding=5
        ))

        return styles
    
    def _get_index_status(self, scans, tuples_read, tuples_fetched, size_bytes):
        """Classify index usage status based on scans and efficiency."""
        try:
            scans_val = int(scans or 0)
        except Exception:
            scans_val = 0
        try:
            read_val = int(tuples_read or 0)
        except Exception:
            read_val = 0
        try:
            fetched_val = int(tuples_fetched or 0)
        except Exception:
            fetched_val = 0
        # size_bytes not used in current thresholds, but kept for future
        if scans_val == 0:
            return "UNUSED"
        elif scans_val < 10:
            return "LOW USE"
        elif (fetched_val / max(read_val, 1)) < 0.1:
            return "INEFFICIENT"
        else:
            return "ACTIVE"
    
    def _get_status_color(self, status_text):
        """Get color based on status text"""
        status_lower = status_text.lower()
        if 'critical' in status_lower or 'unused' in status_lower:
            return colors.HexColor('#e74c3c')  # Red
        elif 'warning' in status_lower or 'low use' in status_lower or 'inefficient' in status_lower:
            return colors.HexColor('#f39c12')  # Orange
        elif 'good' in status_lower or 'active' in status_lower or 'excellent' in status_lower:
            return colors.HexColor('#27ae60')  # Green
        else:
            return colors.black
    
    def _create_header(self):
        """Create professional header with company branding"""
        header_elements = []
        
        # Main title
        header_elements.append(Paragraph("Database Performance Monitoring Report", self.styles['CustomTitle']))
        header_elements.append(Spacer(1, 10))
        
        # Report metadata
        report_date = datetime.now().strftime("%B %d, %Y at %H:%M")
        header_elements.append(Paragraph(f"Generated on: {report_date}", self.styles['Normal']))
        
        config_file = Path(__file__).parent.parent / "config" / "db_config.yaml"
        with open(config_file, "r") as f:
            db = yaml.safe_load(f)["database"]
            header_elements.append(Paragraph(f"DB: {db['dbname']}", self.styles['Normal']))
            header_elements.append(Paragraph(f"Host: {db['host']}:{db['port']}", self.styles['Normal']))
            header_elements.append(Paragraph(f"User: {db['user']}", self.styles['Normal']))
            header_elements.append(Spacer(1, 20))
        
        return header_elements
    
    def _create_executive_summary(self, report_data):
        """Create executive summary with key metrics"""
        summary_elements = []
        summary_elements.append(Paragraph("Executive Summary", self.styles['SectionHeader']))
        
        # Extract key metrics
        cpu_ram = report_data.get("CPU/RAM Usage", {})
        cache_data = report_data.get("Cache Hit Ratio", {})
        long_queries = report_data.get("Long Queries", [])
        
        # Get total cache ratio
        total_cache = cache_data.get('total', ())
        cache_ratio = float(total_cache[2]) * 100 if total_cache and len(total_cache) > 2 else 0
        
        # Create summary table with colored status cells
        slow_queries_count = len(long_queries)
        summary_data = [
            ['Metric', 'Current Value', 'Status'],
            ['System CPU Usage', f"{cpu_ram.get('system_cpu_percent', 0):.1f}%", 
             self._get_status_indicator(cpu_ram.get('system_cpu_percent', 0), 80, 90)],
            ['System RAM Usage', f"{cpu_ram.get('system_ram_percent', 0):.1f}%", 
             self._get_status_indicator(cpu_ram.get('system_ram_percent', 0), 80, 90)],
            ['PostgreSQL Connections', f"{cpu_ram.get('postgres_active_connections', 0)}", 
             self._get_status_indicator(cpu_ram.get('postgres_active_connections', 0), 50, 100)],
            ['Cache Hit Ratio', f"{cache_ratio:.1f}%", 
             self._status_higher_is_better(cache_ratio, warn=90, crit=80)],
            ['Slow Queries', f"{slow_queries_count} queries", 
             self._status_count(slow_queries_count, warn=5, crit=10)]
        ]
        
        summary_table = Table(summary_data, colWidths=[2*inch, 1.5*inch, 1.5*inch])
        
        # Create table style with conditional formatting
        table_style = [
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498db')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
        ]
        
        # Add colored backgrounds and text colors for status column
        for i in range(1, len(summary_data)):
            status_text = summary_data[i][2]
            text_color = self._get_status_color(status_text)
            table_style.append(('TEXTCOLOR', (2, i), (2, i), text_color))
            table_style.append(('FONTNAME', (2, i), (2, i), 'Helvetica-Bold'))
        
        summary_table.setStyle(TableStyle(table_style))
        
        summary_elements.append(summary_table)
        summary_elements.append(Spacer(1, 20))
        
        return summary_elements
    
    def _get_status_indicator(self, value, warning_threshold, critical_threshold):
        """Get status indicator based on value (higher is worse)."""
        try:
            v = float(value)
        except Exception:
            v = 0.0
        if v >= critical_threshold:
            return "CRITICAL"
        elif v >= warning_threshold:
            return "WARNING"
        else:
            return "GOOD"

    def _status_higher_is_better(self, value, warn, crit):
        """For metrics where higher is better (e.g., cache hit %)."""
        try:
            v = float(value)
        except Exception:
            v = 0.0
        if v <= crit:
            return "CRITICAL"
        elif v <= warn:
            return "WARNING"
        else:
            return "EXCELLENT"

    def _status_count(self, count_value, warn, crit):
        """For counts where higher is worse (e.g., slow queries count)."""
        try:
            v = float(count_value)
        except Exception:
            v = 0.0
        if v >= crit:
            return "CRITICAL"
        elif v >= warn:
            return "WARNING"
        else:
            return "GOOD"
    
    def _create_system_metrics_chart(self, cpu_ram_data):
        """Create comprehensive system metrics chart"""
        if not cpu_ram_data:
            return None
            
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 8))
        
        # System CPU Usage
        system_cpu = cpu_ram_data.get('system_cpu_percent', 0)
        ax1.bar(['System CPU'], [system_cpu], color='#3498db', alpha=0.8)
        ax1.set_ylim(0, 100)
        ax1.set_ylabel('Percentage (%)')
        ax1.set_title('System CPU Usage', fontsize=12, fontweight='bold')
        ax1.grid(True, alpha=0.3)
        ax1.axhline(y=80, color='orange', linestyle='--', alpha=0.7)
        ax1.axhline(y=90, color='red', linestyle='--', alpha=0.7)
        
        # System RAM Usage
        system_ram = cpu_ram_data.get('system_ram_percent', 0)
        ax2.bar(['System RAM'], [system_ram], color='#e74c3c', alpha=0.8)
        ax2.set_ylim(0, 100)
        ax2.set_ylabel('Percentage (%)')
        ax2.set_title('System RAM Usage', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        ax2.axhline(y=80, color='orange', linestyle='--', alpha=0.7)
        ax2.axhline(y=90, color='red', linestyle='--', alpha=0.7)
        
        # PostgreSQL Memory Configuration
        postgres_memory = [
            cpu_ram_data.get('postgres_shared_buffers_mb', 0),
            cpu_ram_data.get('postgres_work_mem_mb', 0),
            cpu_ram_data.get('postgres_maintenance_work_mem_mb', 0)
        ]
        memory_labels = ['Shared Buffers', 'Work Memory', 'Maintenance Work Memory']
        ax3.bar(memory_labels, postgres_memory, color='#9b59b6', alpha=0.8)
        ax3.set_ylabel('Memory (MB)')
        ax3.set_title('PostgreSQL Memory Configuration', fontsize=12, fontweight='bold')
        ax3.grid(True, alpha=0.3)
        plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')
        
        # Active Connections
        active_conn = cpu_ram_data.get('postgres_active_connections', 0)
        active_queries = cpu_ram_data.get('postgres_active_queries', 0)
        ax4.bar(['Active Connections', 'Active Queries'], [active_conn, active_queries], 
                color=['#f39c12', '#27ae60'], alpha=0.8)
        ax4.set_ylabel('Count')
        ax4.set_title('PostgreSQL Activity', fontsize=12, fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save to bytes
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        plt.close()
        
        return img_buffer
    
    def _create_cache_charts(self, cache_data):
        """Create comprehensive cache analysis charts"""
        if not cache_data:
            return None
            
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Total Cache Hit Ratio
        total_cache = cache_data.get('total', ())
        if total_cache and len(total_cache) >= 3:
            hits, reads, ratio = total_cache
            try:
                ratio = float(ratio)
            except Exception:
                ratio = 0.0
            miss_ratio = max(0.0, 1 - ratio)
            sizes = [ratio, miss_ratio]
            labels = ['Cache Hits', 'Cache Misses']
            colors_pie = ['#27ae60', '#e74c3c']
            explode = (0.05, 0)
            ax1.pie(sizes, explode=explode, labels=labels, colors=colors_pie,
                    autopct='%1.1f%%', shadow=True, startangle=90)
            ax1.set_title('Total Cache Hit Ratio', fontsize=14, fontweight='bold')
            # Add performance indicator
            if ratio >= 0.95:
                status = "Excellent"; status_color = '#27ae60'
            elif ratio >= 0.90:
                status = "Good"; status_color = '#f39c12'
            else:
                status = "Needs Attention"; status_color = '#e74c3c'
            ax1.text(0, -1.3, f'Status: {status}', ha='center', fontsize=10,
                     fontweight='bold', color=status_color)
        else:
            ax1.axis('off')
            ax1.text(0.5, 0.5, 'No total cache data', ha='center', va='center', fontsize=11)
        
        # Index vs Heap Ratio (Top 10 tables)
        index_heap_data = cache_data.get('index_heap_ratio', [])
        if index_heap_data:
            table_names = [f"{row[0]}.{row[1]}" for row in index_heap_data[:10]]
            ratios = []
            for row in index_heap_data[:10]:
                try:
                    ratios.append(float(row[4]))
                except Exception:
                    ratios.append(0.0)
            bars = ax2.barh(table_names, ratios, color='#8e44ad', alpha=0.8)
            ax2.set_xlabel('Index Ratio (%)  —  formula: index / total')
            ax2.set_title('Index vs Heap Usage Ratio (Top 10 Tables)', fontsize=12, fontweight='bold')
            # Show explicit formula on the chart for clarity
            ax2.text(
                0.5, 1.04,
                'index / total = (idx_blks_hit + idx_blks_read) / (heap_blks_hit + heap_blks_read + idx_blks_hit + idx_blks_read)',
                transform=ax2.transAxes, ha='center', va='bottom', fontsize=8, color='#2c3e50'
            )
            ax2.grid(True, alpha=0.3, axis='x')
            # Add value labels
            max_ratio = max(ratios) if ratios else 0
            x_right = (max_ratio * 1.12) if max_ratio > 0 else 1
            ax2.set_xlim(0, x_right)
            for bar, ratio in zip(bars, ratios):
                ax2.text(min(bar.get_width() + x_right*0.01, x_right*0.98),
                         bar.get_y() + bar.get_height()/2,
                         f'{ratio:.1f}%', va='center', fontsize=8)
        else:
            ax2.axis('off')
            ax2.text(0.5, 0.5, 'No index/heap ratio data', ha='center', va='center', fontsize=11)
        
        plt.tight_layout()
        
        # Save to bytes
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        plt.close()
        
        return img_buffer
    
    def _create_storage_charts(self, storage_data):
        """Create comprehensive storage usage charts"""
        if not storage_data:
            return None
            
        # Use GridSpec with explicit spacing to avoid overlaps between subplots
        fig = plt.figure(figsize=(11, 12))
        gs = fig.add_gridspec(3, 1, height_ratios=[1.0, 1.0, 1.2], hspace=0.48)
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1])
        ax3 = fig.add_subplot(gs[2])
        
        # Database Storage
        db_data = storage_data.get('databases', [])
        if db_data:
            db_names = [row[0] for row in db_data]
            # Use bytes (row[2]) and convert to GB
            db_sizes_gb = [float(row[2]) / (1024**3) for row in db_data]
            
            bars1 = ax1.bar(db_names, db_sizes_gb, color='#9b59b6', alpha=0.8)
            ax1.set_ylabel('Size (GB)')
            ax1.set_title('Database Storage Usage', fontsize=14, fontweight='bold')
            ax1.grid(True, alpha=0.3, axis='y')
            plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')
            
            # Add value labels
            for bar, size_gb in zip(bars1, db_sizes_gb):
                height = bar.get_height()
                ax1.text(
                    bar.get_x() + bar.get_width()/2.,
                    height + (max(db_sizes_gb) * 0.01 if max(db_sizes_gb) > 0 else 0.05),
                    self._format_bytes_prefer_gb(size_gb * (1024**3)),
                    ha='center', va='bottom', fontsize=8
                )
        
        # Table Storage (Top 10)
        table_data = storage_data.get('tables', [])
        if table_data:
            table_names = []
            for row in table_data[:10]:
                nm = f"{row[0]}.{row[1]}"
                nm = (nm[:28] + '…') if len(nm) > 29 else nm
                table_names.append(nm)
            # Use total_size_bytes (row[5]) and convert to GB
            table_sizes_gb = [float(row[5]) / (1024**3) for row in table_data[:10]]
            
            bars2 = ax2.barh(table_names, table_sizes_gb, color='#e67e22', alpha=0.8)
            ax2.set_xlabel('Size (GB)')
            ax2.set_title('Table Storage Usage (Top 10)', fontsize=14, fontweight='bold')
            ax2.grid(True, alpha=0.3, axis='x')
            
            # Add value labels
            for bar, size_gb in zip(bars2, table_sizes_gb):
                ax2.text(
                    bar.get_width() + (max(table_sizes_gb) * 0.01 if max(table_sizes_gb) > 0 else 0.05),
                    bar.get_y() + bar.get_height()/2,
                    self._format_bytes_prefer_gb(size_gb * (1024**3)),
                    va='center', fontsize=8
                )

        # Row counts per table (Top 10 by rows)
        table_data_rows = storage_data.get('tables', [])
        if table_data_rows:
            # Collect name and row count safely
            name_count_pairs = []
            for r in table_data_rows:
                try:
                    row_cnt = int(r[7]) if len(r) > 7 and r[7] is not None else 0
                except Exception:
                    row_cnt = 0
                name = f"{r[0]}.{r[1]}"
                # Truncate long labels to avoid overflow
                name = (name[:22] + '…') if len(name) > 23 else name
                name_count_pairs.append((name, row_cnt))
            # Sort and take top 10 by row count
            name_count_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = name_count_pairs[:10]
            if top_pairs:
                table_names_rows = [p[0] for p in top_pairs][::-1]
                row_counts = [p[1] for p in top_pairs][::-1]
                total_rows = sum(row_counts)
                if total_rows > 0:
                    bars3 = ax3.barh(table_names_rows, row_counts, color='#16a085', alpha=0.85)
                    ax3.set_xlabel('Row Count')
                    ax3.set_title('Row Counts per Table (Top 10)', fontsize=12, fontweight='bold')
                    ax3.grid(True, alpha=0.3, axis='x')
                    # Compact x-axis for thousands/millions
                    try:
                        from matplotlib.ticker import FuncFormatter
                        ax3.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1_000_000:.1f}M" if abs(x) >= 1_000_000 else (f"{x/1_000:.0f}k" if abs(x) >= 1_000 else f"{int(x)}")))
                    except Exception:
                        pass
                    ax3.margins(x=0.05)
                    # Add some headroom on the right for end labels
                    max_rows = max(row_counts) if row_counts else 1
                    ax3.set_xlim(0, max_rows * 1.12)
                    # Rely on GridSpec spacing; no manual axis repositioning
                    # Place value labels at the end of each bar in black
                    x_right = ax3.get_xlim()[1]
                    for bar, cnt in zip(bars3, row_counts):
                        width = bar.get_width()
                        label_x = min(width + (x_right * 0.01), x_right * 0.98)
                        ax3.text(
                            label_x,
                            bar.get_y() + bar.get_height()/2,
                            f"{cnt:,}", va='center', ha='left', fontsize=8, color='black'
                        )
                else:
                    # No meaningful row counts, show message and avoid empty bars
                    ax3.axis('off')
                    ax3.text(0.5, 0.5, 'No row count data available', ha='center', va='center', fontsize=11)
        
        # Constrained layout is enabled; avoid manual tight_layout/subplots_adjust here
        
        # Save to bytes
        img_buffer = io.BytesIO()
        # Lower DPI and limit figure size to avoid huge images and decompression warnings
        plt.savefig(img_buffer, format='png', dpi=130, bbox_inches='tight')
        img_buffer.seek(0)
        plt.close()
        
        return img_buffer
    
    def _parse_size_to_mb(self, size_str):
        """Deprecated: kept for backward compatibility."""
        if not size_str:
            return 0
        size_str = str(size_str).upper()
        if 'TB' in size_str:
            return float(size_str.replace('TB', '').strip()) * 1024 * 1024
        elif 'GB' in size_str:
            return float(size_str.replace('GB', '').strip()) * 1024
        elif 'MB' in size_str:
            return float(size_str.replace('MB', '').strip())
        elif 'KB' in size_str:
            return float(size_str.replace('KB', '').strip()) / 1024
        else:
            return 0

    def _format_bytes_prefer_gb(self, size_bytes):
        """Format a byte value preferring GB when appropriate; fallback to MB/KB."""
        try:
            size = float(size_bytes)
        except Exception:
            return '0'
        gb = size / (1024**3)
        if gb >= 1:
            return f"{gb:.2f} GB"
        mb = size / (1024**2)
        if mb >= 1:
            return f"{mb:.1f} MB"
        kb = size / 1024
        return f"{kb:.0f} KB"
    
    def _format_query_for_table(self, query, max_length=300):
        """Format query text for table display with intelligent truncation"""
        if len(query) <= max_length:
            return query
        
        # For very long queries, try to preserve the beginning and show it's truncated
        # Find a good break point near the max length
        break_point = max_length - 50  # Leave room for "... [truncated]"
        
        # Look for a good break point (end of statement, semicolon, etc.)
        # Priority: semicolon, then closing parentheses, then spaces
        for i in range(break_point, max(0, break_point - 100), -1):
            if query[i] in [';']:
                break_point = i + 1
                break
            elif query[i] in [')', ']', '}']:
                break_point = i + 1
                break
            elif query[i] in [' '] and i > break_point - 20:
                break_point = i
                break
        
        # If no good break point found, just truncate at max_length
        if break_point < max_length - 50:
            break_point = max_length - 50
            
        truncated = query[:break_point]
        return truncated + "... [truncated]"
    
    def _create_queries_table(self, queries_data, title, max_rows=10):
        """Create professional table for queries data with full query text"""
        if not queries_data:
            return [Paragraph(f"{title}: No data available", self.styles['Normal'])]
        
        # Prepare table data
        table_data = []
        
        if title == "Long Running Queries":
            headers = ['Query', 'Total Time', 'Avg Time', 'Calls']
            for row in queries_data[:max_rows]:
                # Use formatted time columns if available, otherwise format manually
                if len(row) >= 6:  # New format with formatted times
                    total_time = row[4]  # total_time_formatted
                    avg_time = row[5]    # mean_time_formatted
                else:  # Old format
                    total_time = f"{row[1]:.2f}ms" if row[1] < 1000 else f"{row[1]/1000:.2f}s"
                    avg_time = f"{row[2]:.2f}ms" if row[2] < 1000 else f"{row[2]/1000:.2f}s"
                
                # Format query for table display
                query_text = self._format_query_for_table(row[0])
                table_data.append([query_text, total_time, avg_time, str(row[3])])
        else:  # Frequent Queries
            headers = ['Query', 'Calls', 'Total Time', 'Avg Time/Call']
            for row in queries_data[:max_rows]:
                # Use formatted time columns if available
                if len(row) >= 5:  # New format with formatted times
                    total_time = row[3]  # total_time_formatted
                    avg_time = row[4]    # avg_time_per_call
                else:  # Old format
                    total_time = f"{row[2]:.2f}ms" if row[2] < 1000 else f"{row[2]/1000:.2f}s"
                    avg_time = f"{row[2]/row[1]:.2f}ms" if row[1] > 0 else "0ms"
                
                # Format query for table display
                query_text = self._format_query_for_table(row[0])
                table_data.append([query_text, str(row[1]), total_time, avg_time])
        
        # Add headers
        table_data.insert(0, headers)
        
        # Create table with appropriate column widths to prevent overlap
        # Total page width is about 7.5 inches, so we need to fit within that
        if title == "Long Running Queries":
            col_widths = [4.2*inch, 0.9*inch, 0.9*inch, 0.7*inch]  # Total: 6.7 inches
        else:
            col_widths = [3.8*inch, 0.7*inch, 1.1*inch, 1.1*inch]  # Total: 6.7 inches
            
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 8),  # Keep normal font size
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('WORDWRAP', (0, 1), (0, -1), 'CJK'),  # Enable word wrapping for query column
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ]))
        
        return [Paragraph(title, self.styles['Subsection']), table]
    
    def _create_storage_tables(self, storage_data):
        """Create detailed storage tables"""
        tables = []
        
        # Database Storage Table
        db_data = storage_data.get('databases', [])
        if db_data:
            db_table_data = [['Database', 'Size']]
            for row in db_data:
                # row: (datname, size_pretty, size_bytes)
                db_table_data.append([row[0], self._format_bytes_prefer_gb(row[2])])
            
            db_table = Table(db_table_data, colWidths=[3*inch, 2*inch])
            db_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9b59b6')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))
            tables.extend([Paragraph("Database Storage", self.styles['Subsection']), db_table, Spacer(1, 15)])
        
        # Table Storage Table
        table_data = storage_data.get('tables', [])
        if table_data:
            table_table_data = [['Schema.Table', 'Total Size', 'Table Size', 'Index Size', 'Row Count (approx)']]
            for row in table_data[:15]:  # Limit to top 15
                # row: (schema, table, total_pretty, table_pretty, index_pretty, total_bytes, table_bytes)
                total_bytes = row[5]
                table_bytes = row[6]
                index_bytes = (row[5] - row[6]) if (row[5] is not None and row[6] is not None) else None
                table_table_data.append([
                    f"{row[0]}.{row[1]}",
                    self._format_bytes_prefer_gb(total_bytes),
                    self._format_bytes_prefer_gb(table_bytes),
                    self._format_bytes_prefer_gb(index_bytes if index_bytes is not None else 0),
                    f"{int(row[7]):,}" if len(row) > 7 and row[7] is not None else '0'
                ])
            
            table_table = Table(table_table_data, colWidths=[2.3*inch, 1.1*inch, 1.1*inch, 1.1*inch, 1.1*inch])
            table_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e67e22')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
            ]))
            tables.extend([Paragraph("Table Storage (Top 15)", self.styles['Subsection']), table_table, Spacer(1, 15)])
        
        # Index Storage Table (with usage status)
        index_data = storage_data.get('indexes', [])
        if index_data:
            # Build usage lookup: (schema, indexname) -> (scans, tup_read, tup_fetch, size_bytes)
            usage_list = storage_data.get('index_usage', [])
            usage_map = {}
            for u in usage_list:
                # u: schemaname, tablename, indexname, size_pretty, size_bytes, idx_scan, idx_tup_read, idx_tup_fetch
                usage_map[(str(u[0]), str(u[2]))] = (u[5], u[6], u[7], u[4])

            index_table_data = [['Schema.Index', 'Table', 'Size', 'Scans', 'Status']]
            for row in index_data[:15]:  # Limit to top 15
                schema = str(row[0])
                indexname = str(row[1])
                table = str(row[2])
                size_pretty = row[3]
                size_bytes = None
                scans = 0
                tup_read = 0
                tup_fetch = 0
                if (schema, indexname) in usage_map:
                    scans, tup_read, tup_fetch, size_bytes = usage_map[(schema, indexname)]
                status = self._get_index_status(scans, tup_read, tup_fetch, size_bytes)
                index_table_data.append([
                    f"{schema}.{indexname}",
                    table,
                    size_pretty,
                    f"{int(scans) if scans is not None else 0}",
                    status
                ])
            
            index_table = Table(index_table_data, colWidths=[2.5*inch, 1.8*inch, 1.0*inch, 0.8*inch, 1.1*inch])
            index_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27ae60')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
            ]))
            tables.extend([Paragraph("Index Storage (Top 15)", self.styles['Subsection']), index_table])
        
        return tables
    
    def _create_cache_tables(self, cache_data):
        """Create detailed cache analysis tables"""
        tables = []
        
        # Per-table Cache Hit Ratio
        per_table_data = cache_data.get('per_table', [])
        if per_table_data:
            cache_table_data = [['Schema.Table', 'Cache Hits', 'Cache Reads', 'Hit Ratio %']]
            for row in per_table_data[:15]:  # Limit to top 15
                cache_table_data.append([f"{row[0]}.{row[1]}", str(row[2]), str(row[3]), f"{float(row[4]):.1f}"])
            
            cache_table = Table(cache_table_data, colWidths=[2.5*inch, 1*inch, 1*inch, 1*inch])
            cache_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f39c12')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
            ]))
            tables.extend([Paragraph("Per-Table Cache Hit Ratios (Worst 15)", self.styles['Subsection']), cache_table])
        
        return tables
    
    def _create_replication_table(self, replication_data):
        """Create replication status table"""
        if not replication_data:
            return [Paragraph("Replication Status: No replication data available", self.styles['Normal'])]
        
        table_data = [['Client Address', 'State', 'Write Lag', 'Flush Lag', 'Replay Lag']]
        
        for row in replication_data:
            table_data.append([
                str(row[0]) if row[0] else 'N/A',
                str(row[1]) if row[1] else 'N/A',
                str(row[2]) if row[2] else 'N/A',
                str(row[3]) if row[3] else 'N/A',
                str(row[4]) if row[4] else 'N/A'
            ])
        
        table = Table(table_data, colWidths=[1.5*inch, 1*inch, 1*inch, 1*inch, 1*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#8e44ad')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
        ]))
        
        return [Paragraph("Replication Status", self.styles['Subsection']), table]
    
    def generate_pdf(self, report_data):
        """Generate the complete professional PDF report"""
        # Add header
        self.elements.extend(self._create_header())
        
        # Add executive summary
        self.elements.extend(self._create_executive_summary(report_data))
        self.elements.append(PageBreak())
        
        # System Performance Section
        self.elements.append(Paragraph("System Performance", self.styles['SectionHeader']))
        
        # System Metrics Chart
        system_chart = self._create_system_metrics_chart(report_data.get("CPU/RAM Usage", {}))
        if system_chart:
            self.elements.append(Image(system_chart, width=7*inch, height=5*inch))
            self.elements.append(Spacer(1, 20))
        
        # Cache Analysis Charts
        cache_charts = self._create_cache_charts(report_data.get("Cache Hit Ratio", {}))
        if cache_charts:
            self.elements.append(Image(cache_charts, width=7*inch, height=3*inch))
            self.elements.append(Spacer(1, 20))
        
        # Storage Usage Charts
        storage_charts = self._create_storage_charts(report_data.get("Storage Usage", {}))
        if storage_charts:
            self.elements.append(Image(storage_charts, width=7*inch, height=5*inch))
            self.elements.append(PageBreak())
        
        # Database Queries Section
        self.elements.append(Paragraph("Database Performance", self.styles['SectionHeader']))
        
        # Long Queries Table with threshold note
        long_threshold_ms = 600
        self.elements.append(Paragraph(f"Slow query threshold: {long_threshold_ms} ms", self.styles['Normal']))
        self.elements.extend(self._create_queries_table(
            report_data.get("Long Queries", []), "Long Running Queries"))
        self.elements.append(Spacer(1, 20))
        
        # Frequent Queries Table
        self.elements.extend(self._create_queries_table(
            report_data.get("Frequent Queries", []), "Most Frequent Queries"))
        self.elements.append(Spacer(1, 20))
        
        # Replication Status
        self.elements.extend(self._create_replication_table(
            report_data.get("Replication Delay", [])))
        self.elements.append(PageBreak())
        
        # Detailed Storage Analysis
        self.elements.append(Paragraph("Detailed Storage Analysis", self.styles['SectionHeader']))
        self.elements.extend(self._create_storage_tables(report_data.get("Storage Usage", {})))
        self.elements.append(PageBreak())
        
        # Detailed Cache Analysis
        self.elements.append(Paragraph("Detailed Cache Analysis", self.styles['SectionHeader']))
        self.elements.extend(self._create_cache_tables(report_data.get("Cache Hit Ratio", {})))
        
        # Build the PDF
        self.doc.build(self.elements)
        print(f"✅ Professional report generated: {self.filename}")

# Backward compatibility function
def generate_pdf(report_data, filename="report.pdf"):
    """Legacy function for backward compatibility"""
    generator = ProfessionalPDFGenerator(filename)
    generator.generate_pdf(report_data)