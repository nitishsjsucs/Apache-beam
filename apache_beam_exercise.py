"""
Apache Beam Data Engineering Exercise
Comprehensive demonstration of Apache Beam features:
1. Composite Transforms
2. Pipeline I/O
3. ParDo
4. Windowing
5. Map, Filter, Partition

Scenario: E-commerce Transaction Processing System
"""

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
import json
from datetime import datetime, timedelta
import random
from typing import Tuple, List, Dict


# ============================================
# DATA GENERATION
# ============================================

def generate_sample_transactions(num_transactions=200):
    """Generate sample e-commerce transactions"""
    products = [
        ('Laptop', 1200, 'Electronics'),
        ('Smartphone', 800, 'Electronics'),
        ('Headphones', 150, 'Electronics'),
        ('Book', 25, 'Books'),
        ('Desk Chair', 300, 'Furniture'),
        ('Coffee Maker', 80, 'Appliances'),
        ('Running Shoes', 120, 'Sports'),
        ('Backpack', 60, 'Accessories'),
        ('Monitor', 400, 'Electronics'),
        ('Keyboard', 100, 'Electronics')
    ]
    
    customers = [f'CUST{str(i).zfill(4)}' for i in range(1, 51)]
    transactions = []
    base_time = datetime.now()
    
    for i in range(num_transactions):
        product_name, price, category = random.choice(products)
        quantity = random.randint(1, 5)
        timestamp = base_time - timedelta(hours=random.randint(0, 24), minutes=random.randint(0, 59))
        
        transaction = {
            'transaction_id': f'TXN{str(i+1).zfill(6)}',
            'customer_id': random.choice(customers),
            'product_name': product_name,
            'category': category,
            'price': price,
            'quantity': quantity,
            'total_amount': price * quantity,
            'timestamp': timestamp.isoformat(),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash']),
            'region': random.choice(['North', 'South', 'East', 'West'])
        }
        transactions.append(transaction)
    
    return transactions


# ============================================
# PARDO: DoFn Classes
# ============================================

class ParseTransactionFn(beam.DoFn):
    """Parse JSON transaction strings"""
    def process(self, element):
        try:
            transaction = json.loads(element)
            yield transaction
        except json.JSONDecodeError as e:
            print(f"Error parsing: {e}")


class EnrichTransactionFn(beam.DoFn):
    """Enrich transactions with computed fields"""
    def process(self, element):
        total = element['total_amount']
        
        # Calculate discount
        if total > 1000:
            discount_rate = 0.15
        elif total > 500:
            discount_rate = 0.10
        elif total > 200:
            discount_rate = 0.05
        else:
            discount_rate = 0.0
        
        element['discount_rate'] = discount_rate
        element['discount_amount'] = total * discount_rate
        element['final_amount'] = total - element['discount_amount']
        
        # Customer tier
        if total > 1000:
            element['customer_tier'] = 'Premium'
        elif total > 500:
            element['customer_tier'] = 'Gold'
        elif total > 200:
            element['customer_tier'] = 'Silver'
        else:
            element['customer_tier'] = 'Bronze'
        
        yield element


class ExtractCategoryAmountFn(beam.DoFn):
    """Extract category and amount as key-value pairs"""
    def process(self, element):
        yield (element['category'], element['final_amount'])


class AddTimestampFn(beam.DoFn):
    """Add timestamps for windowing"""
    def process(self, element):
        timestamp = datetime.fromisoformat(element['timestamp'])
        unix_timestamp = timestamp.timestamp()
        yield window.TimestampedValue(element, unix_timestamp)


# ============================================
# COMPOSITE TRANSFORMS
# ============================================

class AnalyzeCustomerSpending(beam.PTransform):
    """Composite Transform: Analyze customer spending patterns"""
    def expand(self, pcoll):
        return (
            pcoll
            | 'Extract Customer Amount' >> beam.Map(
                lambda x: (x['customer_id'], x['final_amount'])
            )
            | 'Group By Customer' >> beam.GroupByKey()
            | 'Calculate Stats' >> beam.Map(
                lambda x: {
                    'customer_id': x[0],
                    'total_spent': sum(x[1]),
                    'num_transactions': len(list(x[1])),
                    'avg_transaction': sum(x[1]) / len(list(x[1]))
                }
            )
        )


class CategorySalesAnalysis(beam.PTransform):
    """Composite Transform: Analyze sales by category"""
    def expand(self, pcoll):
        return (
            pcoll
            | 'Extract Category Amount' >> beam.ParDo(ExtractCategoryAmountFn())
            | 'Group By Category' >> beam.GroupByKey()
            | 'Sum By Category' >> beam.Map(
                lambda x: {
                    'category': x[0],
                    'total_sales': sum(x[1]),
                    'num_items': len(list(x[1])),
                    'avg_sale': sum(x[1]) / len(list(x[1]))
                }
            )
        )


# ============================================
# PARTITION FUNCTION
# ============================================

def partition_by_amount(element, num_partitions):
    """
    Partition transactions:
    0: Small (< $200)
    1: Medium ($200 - $1000)
    2: Large (> $1000)
    """
    amount = element['final_amount']
    if amount < 200:
        return 0
    elif amount <= 1000:
        return 1
    else:
        return 2


# ============================================
# MAIN PIPELINE
# ============================================

def run_comprehensive_pipeline():
    """Main pipeline demonstrating all Apache Beam features"""
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as pipeline:
        
        # PIPELINE I/O: Read from file
        raw_transactions = (
            pipeline
            | 'Read Transactions' >> beam.io.ReadFromText('transactions.json')
        )
        
        # PARDO: Parse and enrich
        parsed_transactions = (
            raw_transactions
            | 'Parse JSON' >> beam.ParDo(ParseTransactionFn())
        )
        
        enriched_transactions = (
            parsed_transactions
            | 'Enrich Transactions' >> beam.ParDo(EnrichTransactionFn())
        )
        
        # MAP: Simple transformations
        transaction_summaries = (
            enriched_transactions
            | 'Create Summary' >> beam.Map(
                lambda x: f"{x['transaction_id']}: {x['customer_id']} spent ${x['final_amount']:.2f}"
            )
        )
        
        # FILTER: High-value transactions
        high_value_transactions = (
            enriched_transactions
            | 'Filter High Value' >> beam.Filter(lambda x: x['final_amount'] > 500)
        )
        
        electronics_only = (
            enriched_transactions
            | 'Filter Electronics' >> beam.Filter(lambda x: x['category'] == 'Electronics')
        )
        
        # PARTITION: Split by size
        small, medium, large = (
            enriched_transactions
            | 'Partition By Amount' >> beam.Partition(partition_by_amount, 3)
        )
        
        # COMPOSITE TRANSFORM: Customer Analysis
        customer_analysis = (
            enriched_transactions
            | 'Analyze Customer Spending' >> AnalyzeCustomerSpending()
        )
        
        # COMPOSITE TRANSFORM: Category Analysis
        category_analysis = (
            enriched_transactions
            | 'Analyze Category Sales' >> CategorySalesAnalysis()
        )
        
        # PIPELINE I/O: Write outputs
        enriched_transactions | 'Write Enriched' >> beam.io.WriteToText(
            'output/enriched_transactions', file_name_suffix='.json', shard_name_template=''
        )
        
        transaction_summaries | 'Write Summaries' >> beam.io.WriteToText(
            'output/transaction_summaries', file_name_suffix='.txt', shard_name_template=''
        )
        
        high_value_transactions | 'Write High Value' >> beam.io.WriteToText(
            'output/high_value_transactions', file_name_suffix='.json', shard_name_template=''
        )
        
        small | 'Write Small' >> beam.io.WriteToText(
            'output/small_transactions', file_name_suffix='.json', shard_name_template=''
        )
        
        medium | 'Write Medium' >> beam.io.WriteToText(
            'output/medium_transactions', file_name_suffix='.json', shard_name_template=''
        )
        
        large | 'Write Large' >> beam.io.WriteToText(
            'output/large_transactions', file_name_suffix='.json', shard_name_template=''
        )
        
        customer_analysis | 'Write Customer Analysis' >> beam.io.WriteToText(
            'output/customer_analysis', file_name_suffix='.json', shard_name_template=''
        )
        
        category_analysis | 'Write Category Analysis' >> beam.io.WriteToText(
            'output/category_analysis', file_name_suffix='.json', shard_name_template=''
        )
    
    print("\n" + "="*60)
    print("Pipeline execution completed successfully!")
    print("="*60)


def run_windowing_pipeline():
    """Pipeline demonstrating windowing operations"""
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as pipeline:
        
        transactions = (
            pipeline
            | 'Read' >> beam.io.ReadFromText('transactions.json')
            | 'Parse' >> beam.ParDo(ParseTransactionFn())
            | 'Enrich' >> beam.ParDo(EnrichTransactionFn())
        )
        
        timestamped_transactions = (
            transactions
            | 'Add Timestamps' >> beam.ParDo(AddTimestampFn())
        )
        
        # WINDOWING: Fixed Windows (1 hour)
        hourly_sales = (
            timestamped_transactions
            | 'Fixed 1-Hour Windows' >> beam.WindowInto(window.FixedWindows(60 * 60))
            | 'Extract Amount' >> beam.Map(lambda x: x['final_amount'])
            | 'Sum Hourly Sales' >> beam.CombineGlobally(sum).without_defaults()
            | 'Format Hourly' >> beam.Map(lambda x: f"Hourly sales: ${x:.2f}")
        )
        
        # WINDOWING: Sliding Windows
        sliding_category_sales = (
            timestamped_transactions
            | 'Sliding 2-Hour Windows' >> beam.WindowInto(
                window.SlidingWindows(60 * 60 * 2, 60 * 60)
            )
            | 'Extract Category Sales' >> beam.Map(
                lambda x: (x['category'], x['final_amount'])
            )
            | 'Group By Category Window' >> beam.GroupByKey()
            | 'Sum Category Sales' >> beam.Map(
                lambda x: {'category': x[0], 'total_sales': sum(x[1])}
            )
        )
        
        # WINDOWING: Session Windows
        customer_sessions = (
            timestamped_transactions
            | 'Session Windows' >> beam.WindowInto(window.Sessions(30 * 60))
            | 'Extract Customer Session' >> beam.Map(lambda x: (x['customer_id'], 1))
            | 'Count Sessions' >> beam.GroupByKey()
            | 'Format Sessions' >> beam.Map(
                lambda x: {'customer_id': x[0], 'transactions_in_session': len(list(x[1]))}
            )
        )
        
        # Write windowed results
        hourly_sales | 'Write Hourly Sales' >> beam.io.WriteToText(
            'output/hourly_sales', file_name_suffix='.txt', shard_name_template=''
        )
        
        sliding_category_sales | 'Write Sliding Sales' >> beam.io.WriteToText(
            'output/sliding_category_sales', file_name_suffix='.json', shard_name_template=''
        )
        
        customer_sessions | 'Write Customer Sessions' >> beam.io.WriteToText(
            'output/customer_sessions', file_name_suffix='.json', shard_name_template=''
        )
    
    print("\n" + "="*60)
    print("Windowing pipeline execution completed!")
    print("="*60)


# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == '__main__':
    print("Apache Beam Data Engineering Exercise")
    print("="*60)
    
    # Generate sample data
    print("\n1. Generating sample transactions...")
    transactions = generate_sample_transactions(200)
    
    with open('transactions.json', 'w') as f:
        for txn in transactions:
            f.write(json.dumps(txn) + '\n')
    
    print(f"Generated {len(transactions)} transactions")
    print(f"Sample: {json.dumps(transactions[0], indent=2)}")
    
    # Run main pipeline
    print("\n2. Running comprehensive pipeline...")
    run_comprehensive_pipeline()
    
    # Run windowing pipeline
    print("\n3. Running windowing pipeline...")
    run_windowing_pipeline()
    
    print("\n" + "="*60)
    print("All pipelines completed successfully!")
    print("Check the 'output/' directory for results")
    print("="*60)
