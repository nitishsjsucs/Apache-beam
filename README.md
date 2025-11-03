# Apache Beam Data Engineering Exercise

## 📋 Overview
This project demonstrates comprehensive Apache Beam features through an **E-commerce Transaction Processing System**. All required features are implemented with practical, real-world examples.

## ✨ Features Demonstrated

### 1. **Composite Transforms** ✅
- `AnalyzeCustomerSpending`: Combines multiple transforms to analyze customer behavior
- `CategorySalesAnalysis`: Aggregates sales data by product category
- Reusable, modular pipeline components

### 2. **Pipeline I/O** ✅
- **Input**: Reading from JSON files (`ReadFromText`)
- **Output**: Writing to multiple output files with different formats
- Demonstrates file-based data ingestion and export

### 3. **ParDo (Parallel Processing)** ✅
- `ParseTransactionFn`: Parse JSON strings to dictionaries
- `EnrichTransactionFn`: Add computed fields (discounts, customer tiers)
- `ExtractCategoryAmountFn`: Extract key-value pairs
- `AddTimestampFn`: Add timestamps for windowing

### 4. **Windowing** ✅
- **Fixed Windows**: 1-hour windows for hourly sales analysis
- **Sliding Windows**: 2-hour windows with 1-hour slide for trend analysis
- **Session Windows**: 30-minute gap for customer session tracking

### 5. **Map** ✅
- Simple element-wise transformations
- Creating transaction summaries
- Extracting specific fields

### 6. **Filter** ✅
- High-value transaction filtering (>$500)
- Category-specific filtering (Electronics only)
- Conditional data selection

### 7. **Partition** ✅
- Split transactions into 3 categories:
  - Small: < $200
  - Medium: $200 - $1000
  - Large: > $1000

## 🚀 Quick Start

### Local Execution

```bash
# Install Apache Beam
pip install apache-beam

# Run the exercise
python apache_beam_exercise.py
```

### Google Colab Execution

1. Upload `apache_beam_exercise.py` to Google Colab
2. Install Apache Beam:
   ```python
   !pip install apache-beam
   ```
3. Run the script:
   ```python
   !python apache_beam_exercise.py
   ```

## 📊 Data Flow

```
Input: transactions.json (200 e-commerce transactions)
    ↓
[Parse JSON] → [Enrich with discounts & tiers]
    ↓
├─→ [Map] → Transaction summaries
├─→ [Filter] → High-value transactions
├─→ [Filter] → Electronics only
├─→ [Partition] → Small/Medium/Large transactions
├─→ [Composite Transform] → Customer spending analysis
├─→ [Composite Transform] → Category sales analysis
└─→ [Windowing] → Time-based aggregations
    ↓
Output: Multiple analysis files in output/ directory
```

## 📁 Project Structure

```
apache-beam/
├── apache_beam_exercise.py      # Main pipeline code
├── README.md                     # This file
├── requirements.txt              # Python dependencies
├── transactions.json             # Generated sample data
└── output/                       # Pipeline outputs
    ├── enriched_transactions.json
    ├── transaction_summaries.txt
    ├── high_value_transactions.json
    ├── small_transactions.json
    ├── medium_transactions.json
    ├── large_transactions.json
    ├── customer_analysis.json
    ├── category_analysis.json
    ├── hourly_sales.txt
    ├── sliding_category_sales.json
    └── customer_sessions.json
```

## 🎯 Key Concepts Explained

### Composite Transforms
Composite transforms encapsulate multiple operations into a single reusable component:
```python
class AnalyzeCustomerSpending(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'Extract' >> beam.Map(...)
            | 'Group' >> beam.GroupByKey()
            | 'Calculate' >> beam.Map(...)
        )
```

### ParDo
ParDo applies a DoFn (Do Function) to each element in parallel:
```python
class EnrichTransactionFn(beam.DoFn):
    def process(self, element):
        # Process each element
        yield enriched_element
```

### Windowing
Windows group elements by time:
```python
# Fixed Windows: Non-overlapping time intervals
beam.WindowInto(window.FixedWindows(60 * 60))  # 1 hour

# Sliding Windows: Overlapping intervals
beam.WindowInto(window.SlidingWindows(7200, 3600))  # 2hr window, 1hr slide

# Session Windows: Activity-based grouping
beam.WindowInto(window.Sessions(1800))  # 30-minute gap
```

### Map, Filter, Partition
```python
# Map: Transform each element
| beam.Map(lambda x: x['amount'])

# Filter: Select elements matching condition
| beam.Filter(lambda x: x['amount'] > 500)

# Partition: Split into multiple outputs
| beam.Partition(partition_fn, 3)
```

## 📈 Sample Outputs

### Customer Analysis
```json
{
  "customer_id": "CUST0001",
  "total_spent": 2450.50,
  "num_transactions": 5,
  "avg_transaction": 490.10
}
```

### Category Analysis
```json
{
  "category": "Electronics",
  "total_sales": 15680.00,
  "num_items": 45,
  "avg_sale": 348.44
}
```

### Windowed Sales
```
Hourly sales: $12,450.75
Hourly sales: $8,920.30
Hourly sales: $15,680.50
```

## 🎥 Video Walkthrough Topics

For your submission video, cover:

1. **Introduction** (2 min)
   - Project overview and scenario
   - Apache Beam features demonstrated

2. **Code Walkthrough** (8 min)
   - Data generation and structure
   - ParDo implementations
   - Composite transforms
   - Map, Filter, Partition examples
   - Windowing operations
   - Pipeline I/O

3. **Execution** (3 min)
   - Running the pipeline
   - Showing generated outputs
   - Explaining results

4. **Key Insights** (2 min)
   - Real-world applications
   - Performance considerations
   - Best practices

## 🔧 Customization

### Modify Transaction Count
```python
transactions = generate_sample_transactions(500)  # Generate 500 transactions
```

### Add Custom Products
```python
products = [
    ('Your Product', price, 'Category'),
    # Add more...
]
```

### Adjust Window Sizes
```python
# Change to 30-minute windows
beam.WindowInto(window.FixedWindows(30 * 60))
```

## 📚 References

- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Beam Python SDK](https://beam.apache.org/documentation/sdks/python/)
- [Interactive Beam Overview](https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/interactive-overview/getting-started.ipynb)

## ✅ Submission Checklist

- [x] Composite Transform implemented
- [x] Pipeline I/O (Read/Write) demonstrated
- [x] ParDo with multiple DoFn classes
- [x] Windowing (Fixed, Sliding, Session)
- [x] Map transformations
- [x] Filter operations
- [x] Partition functionality
- [x] Comprehensive code comments
- [x] README documentation
- [ ] Video walkthrough recorded
- [ ] Colab notebook uploaded

## 🎓 Learning Outcomes

After completing this exercise, you will understand:
- How to structure Apache Beam pipelines
- Parallel data processing with ParDo
- Creating reusable transforms
- Time-based data processing with windows
- Data partitioning and filtering strategies
- Pipeline I/O operations
- Real-world data engineering patterns

## 💡 Tips for Success

1. **Run locally first** to debug quickly
2. **Check output files** after each pipeline run
3. **Experiment with parameters** (window sizes, thresholds)
4. **Add logging** to understand data flow
5. **Test with small datasets** before scaling up

## 🤝 Support

For questions or issues:
- Review Apache Beam documentation
- Check the code comments
- Experiment with smaller examples
- Use print statements for debugging

---

**Created for**: Data Engineering Course  
**Due Date**: Sunday, 23:59  
**Points**: 100  

Good luck! 🚀
