# 🏗️ Apache Beam Pipeline Architecture

## System Overview

This document provides a visual understanding of the data flow and architecture.

---

## 📊 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     INPUT DATA SOURCE                            │
│                   transactions.json                              │
│              (200 E-commerce Transactions)                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PIPELINE I/O: READ                            │
│              beam.io.ReadFromText()                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PARDO: ParseTransactionFn                       │
│            Parse JSON strings → Dictionaries                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PARDO: EnrichTransactionFn                       │
│        Add: discounts, final_amount, customer_tier               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ├──────────────┬──────────────┬──────────┐
                         │              │              │          │
                         ▼              ▼              ▼          ▼
              ┌──────────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐
              │     MAP      │  │  FILTER  │  │ PARTITION│  │COMPOSITE│
              │  Transform   │  │ High Val │  │ 3-way    │  │Transform│
              │  Summaries   │  │ Elec Only│  │ Split    │  │ Analysis│
              └──────┬───────┘  └────┬─────┘  └────┬─────┘  └────┬────┘
                     │               │             │             │
                     ▼               ▼             ▼             ▼
              ┌──────────────────────────────────────────────────────┐
              │           PIPELINE I/O: WRITE (11 files)             │
              └──────────────────────────────────────────────────────┘
```

---

## 🔄 Main Pipeline Flow

```
INPUT: transactions.json
  │
  ├─→ [Read] → Raw JSON strings
  │
  ├─→ [ParDo: Parse] → Python dictionaries
  │
  ├─→ [ParDo: Enrich] → Add computed fields
  │
  ├─→ [MAP] → Create summaries
  │     └─→ OUTPUT: transaction_summaries.txt
  │
  ├─→ [FILTER: High Value] → Transactions > $500
  │     └─→ OUTPUT: high_value_transactions.json
  │
  ├─→ [FILTER: Electronics] → Category = Electronics
  │     └─→ OUTPUT: (used in analysis)
  │
  ├─→ [PARTITION] → Split by amount
  │     ├─→ Small (<$200)
  │     │   └─→ OUTPUT: small_transactions.json
  │     ├─→ Medium ($200-$1000)
  │     │   └─→ OUTPUT: medium_transactions.json
  │     └─→ Large (>$1000)
  │         └─→ OUTPUT: large_transactions.json
  │
  ├─→ [COMPOSITE: Customer Analysis]
  │     ├─→ Extract (customer_id, amount)
  │     ├─→ GroupByKey
  │     └─→ Calculate stats
  │         └─→ OUTPUT: customer_analysis.json
  │
  └─→ [COMPOSITE: Category Analysis]
        ├─→ ParDo: Extract (category, amount)
        ├─→ GroupByKey
        └─→ Sum and average
            └─→ OUTPUT: category_analysis.json
```

---

## ⏰ Windowing Pipeline Flow

```
INPUT: transactions.json
  │
  ├─→ [Read] → [Parse] → [Enrich]
  │
  ├─→ [ParDo: AddTimestamp] → Timestamped elements
  │
  ├─→ [WINDOW: Fixed 1-hour]
  │     ├─→ Extract amounts
  │     ├─→ Sum per window
  │     └─→ OUTPUT: hourly_sales.txt
  │
  ├─→ [WINDOW: Sliding 2hr/1hr]
  │     ├─→ Extract (category, amount)
  │     ├─→ GroupByKey per window
  │     └─→ Sum per category
  │         └─→ OUTPUT: sliding_category_sales.json
  │
  └─→ [WINDOW: Session 30min]
        ├─→ Extract (customer_id, 1)
        ├─→ GroupByKey per session
        └─→ Count transactions
            └─→ OUTPUT: customer_sessions.json
```

---

## 🎯 Feature Implementation Map

### 1. Composite Transform Architecture

```
AnalyzeCustomerSpending (PTransform)
  │
  ├─→ Map: (txn) → (customer_id, amount)
  │
  ├─→ GroupByKey: Group amounts by customer
  │
  └─→ Map: Calculate statistics
        ├─→ total_spent = sum(amounts)
        ├─→ num_transactions = count(amounts)
        └─→ avg_transaction = total / count

CategorySalesAnalysis (PTransform)
  │
  ├─→ ParDo: ExtractCategoryAmountFn
  │     └─→ (txn) → (category, amount)
  │
  ├─→ GroupByKey: Group amounts by category
  │
  └─→ Map: Calculate statistics
        ├─→ total_sales = sum(amounts)
        ├─→ num_items = count(amounts)
        └─→ avg_sale = total / count
```

### 2. ParDo Processing Chain

```
Raw JSON String
  │
  ▼
ParseTransactionFn (DoFn)
  │ process(): json.loads(element)
  ▼
Dictionary
  │
  ▼
EnrichTransactionFn (DoFn)
  │ process(): 
  │   - Calculate discount_rate
  │   - Calculate discount_amount
  │   - Calculate final_amount
  │   - Assign customer_tier
  ▼
Enriched Dictionary
  │
  ▼
ExtractCategoryAmountFn (DoFn)
  │ process(): (category, amount)
  ▼
Key-Value Pair
  │
  ▼
AddTimestampFn (DoFn)
  │ process(): TimestampedValue(element, timestamp)
  ▼
Timestamped Element
```

### 3. Windowing Types

```
FIXED WINDOWS (1 hour)
├─ 00:00 - 01:00 ─┤
                  ├─ 01:00 - 02:00 ─┤
                                    ├─ 02:00 - 03:00 ─┤
Non-overlapping, equal-sized intervals


SLIDING WINDOWS (2 hour window, 1 hour slide)
├────── 00:00 - 02:00 ──────┤
              ├────── 01:00 - 03:00 ──────┤
                            ├────── 02:00 - 04:00 ──────┤
Overlapping intervals for trend analysis


SESSION WINDOWS (30 minute gap)
├─ Activity ─┤ [30min gap] ├─ Activity ─┤ [30min gap] ├─ Activity ─┤
   Session 1                  Session 2                  Session 3
Dynamic windows based on activity
```

### 4. Partition Logic

```
Transaction Amount
        │
        ▼
  partition_by_amount()
        │
        ├─→ amount < $200
        │     └─→ Partition 0 (Small)
        │
        ├─→ $200 ≤ amount ≤ $1000
        │     └─→ Partition 1 (Medium)
        │
        └─→ amount > $1000
              └─→ Partition 2 (Large)
```

---

## 📦 Data Structure Evolution

### Stage 1: Raw Input
```json
{
  "transaction_id": "TXN000001",
  "customer_id": "CUST0023",
  "product_name": "Laptop",
  "category": "Electronics",
  "price": 1200,
  "quantity": 2,
  "total_amount": 2400,
  "timestamp": "2024-11-02T14:30:00",
  "payment_method": "Credit Card",
  "region": "North"
}
```

### Stage 2: After Enrichment
```json
{
  "transaction_id": "TXN000001",
  "customer_id": "CUST0023",
  "product_name": "Laptop",
  "category": "Electronics",
  "price": 1200,
  "quantity": 2,
  "total_amount": 2400,
  "timestamp": "2024-11-02T14:30:00",
  "payment_method": "Credit Card",
  "region": "North",
  "discount_rate": 0.15,           ← ADDED
  "discount_amount": 360.0,         ← ADDED
  "final_amount": 2040.0,           ← ADDED
  "customer_tier": "Premium"        ← ADDED
}
```

### Stage 3: After Customer Analysis (Composite)
```json
{
  "customer_id": "CUST0023",
  "total_spent": 5240.50,
  "num_transactions": 3,
  "avg_transaction": 1746.83
}
```

### Stage 4: After Category Analysis (Composite)
```json
{
  "category": "Electronics",
  "total_sales": 45680.00,
  "num_items": 67,
  "avg_sale": 681.79
}
```

---

## 🔧 Transform Types Summary

| Transform | Type | Input | Output | Purpose |
|-----------|------|-------|--------|---------|
| **ReadFromText** | I/O | File | PCollection[str] | Read data |
| **ParseTransactionFn** | ParDo | str | dict | Parse JSON |
| **EnrichTransactionFn** | ParDo | dict | dict | Add fields |
| **Map** | Transform | Any | Any | 1-to-1 transform |
| **Filter** | Transform | Any | Any | Conditional select |
| **Partition** | Transform | Any | Multiple PCollections | Split data |
| **GroupByKey** | Transform | (K,V) | (K, Iterable[V]) | Group by key |
| **CombineGlobally** | Transform | Any | Single value | Aggregate all |
| **WindowInto** | Transform | Any | Windowed PCollection | Time grouping |
| **AnalyzeCustomerSpending** | Composite | dict | dict | Multi-step analysis |
| **CategorySalesAnalysis** | Composite | dict | dict | Multi-step analysis |
| **WriteToText** | I/O | PCollection | File | Write data |

---

## 🎨 Pipeline Execution Order

```
1. Data Generation
   └─→ generate_sample_transactions(200)
        └─→ Save to transactions.json

2. Main Pipeline Execution
   ├─→ Read transactions.json
   ├─→ Parse JSON (ParDo)
   ├─→ Enrich data (ParDo)
   ├─→ Branch into multiple transforms:
   │   ├─→ Map → Summaries
   │   ├─→ Filter → High value
   │   ├─→ Filter → Electronics
   │   ├─→ Partition → Small/Medium/Large
   │   ├─→ Composite → Customer analysis
   │   └─→ Composite → Category analysis
   └─→ Write 8 output files

3. Windowing Pipeline Execution
   ├─→ Read transactions.json
   ├─→ Parse JSON (ParDo)
   ├─→ Enrich data (ParDo)
   ├─→ Add timestamps (ParDo)
   ├─→ Branch into window types:
   │   ├─→ Fixed windows → Hourly sales
   │   ├─→ Sliding windows → Category trends
   │   └─→ Session windows → Customer sessions
   └─→ Write 3 output files

Total: 11 output files
```

---

## 💾 Output File Mapping

| File | Source Transform | Feature Demonstrated |
|------|------------------|---------------------|
| enriched_transactions.json | ParDo: Enrich | ParDo, I/O |
| transaction_summaries.txt | Map | Map, I/O |
| high_value_transactions.json | Filter | Filter, I/O |
| small_transactions.json | Partition[0] | Partition, I/O |
| medium_transactions.json | Partition[1] | Partition, I/O |
| large_transactions.json | Partition[2] | Partition, I/O |
| customer_analysis.json | Composite: Customer | Composite, I/O |
| category_analysis.json | Composite: Category | Composite, I/O |
| hourly_sales.txt | Window: Fixed | Windowing, I/O |
| sliding_category_sales.json | Window: Sliding | Windowing, I/O |
| customer_sessions.json | Window: Session | Windowing, I/O |

---

## 🔍 Key Architectural Decisions

### 1. **E-commerce Scenario**
- **Why**: Relatable, practical, demonstrates real-world use
- **Benefit**: Easy to explain, clear business value

### 2. **Two Separate Pipelines**
- **Why**: Windowing requires timestamps, cleaner separation
- **Benefit**: Easier to understand, modular design

### 3. **Multiple Output Files**
- **Why**: Demonstrates I/O, shows different analyses
- **Benefit**: Clear results, easy to verify

### 4. **Composite Transforms**
- **Why**: Shows reusability, encapsulation
- **Benefit**: Production-ready pattern, maintainable

### 5. **Rich Data Enrichment**
- **Why**: Demonstrates ParDo capabilities
- **Benefit**: Shows complex transformations

---

## 📈 Scalability Considerations

This pipeline is designed to scale:

```
Current: 200 transactions, local execution
         ↓
Scale: 200K transactions, DirectRunner
         ↓
Scale: 2M transactions, DataflowRunner (GCP)
         ↓
Scale: 20M+ transactions, Distributed Beam
```

**No code changes needed** - just change the runner!

```python
# Local
options = PipelineOptions()

# Google Cloud Dataflow
options = PipelineOptions([
    '--runner=DataflowRunner',
    '--project=my-project',
    '--region=us-central1',
    '--temp_location=gs://my-bucket/temp'
])
```

---

## 🎯 Architecture Highlights

### ✅ Strengths
1. **Modular Design** - Each feature is independent
2. **Clear Separation** - Main vs Windowing pipelines
3. **Reusable Components** - Composite transforms
4. **Comprehensive I/O** - 11 output files
5. **Production Patterns** - DoFn classes, error handling

### 🔄 Data Flow Patterns
1. **Fan-out** - One input → Multiple outputs
2. **Aggregation** - Many elements → Summary statistics
3. **Filtering** - Conditional selection
4. **Partitioning** - Data splitting
5. **Windowing** - Time-based grouping

### 🏗️ Design Principles
1. **Single Responsibility** - Each DoFn does one thing
2. **Composability** - Transforms can be combined
3. **Testability** - Each component can be tested
4. **Readability** - Clear naming, comments
5. **Extensibility** - Easy to add new features

---

This architecture provides a solid foundation for understanding Apache Beam's capabilities and can be extended for more complex use cases.
