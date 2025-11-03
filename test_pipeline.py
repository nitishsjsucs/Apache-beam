"""
Quick test script to verify Apache Beam installation and pipeline execution
"""

import sys

def test_imports():
    """Test if all required imports work"""
    print("Testing imports...")
    try:
        import apache_beam as beam
        from apache_beam import window
        from apache_beam.options.pipeline_options import PipelineOptions
        import json
        from datetime import datetime, timedelta
        import random
        print("✓ All imports successful")
        print(f"✓ Apache Beam version: {beam.__version__}")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        print("\nPlease install Apache Beam:")
        print("  pip install apache-beam")
        return False


def test_simple_pipeline():
    """Test a simple pipeline"""
    print("\nTesting simple pipeline...")
    try:
        import apache_beam as beam
        from apache_beam.options.pipeline_options import PipelineOptions
        
        options = PipelineOptions()
        with beam.Pipeline(options=options) as pipeline:
            result = (
                pipeline
                | 'Create' >> beam.Create([1, 2, 3, 4, 5])
                | 'Square' >> beam.Map(lambda x: x * x)
                | 'Sum' >> beam.CombineGlobally(sum)
            )
        
        print("✓ Simple pipeline executed successfully")
        return True
    except Exception as e:
        print(f"✗ Pipeline error: {e}")
        return False


def test_file_operations():
    """Test file read/write operations"""
    print("\nTesting file operations...")
    try:
        import apache_beam as beam
        from apache_beam.options.pipeline_options import PipelineOptions
        import os
        
        # Create test data
        test_file = 'test_input.txt'
        with open(test_file, 'w') as f:
            f.write("line1\nline2\nline3\n")
        
        # Run pipeline
        options = PipelineOptions()
        with beam.Pipeline(options=options) as pipeline:
            (
                pipeline
                | 'Read' >> beam.io.ReadFromText(test_file)
                | 'Upper' >> beam.Map(str.upper)
                | 'Write' >> beam.io.WriteToText(
                    'test_output',
                    file_name_suffix='.txt',
                    shard_name_template=''
                )
            )
        
        # Check output
        if os.path.exists('test_output.txt'):
            with open('test_output.txt', 'r') as f:
                content = f.read()
                if 'LINE1' in content and 'LINE2' in content:
                    print("✓ File operations successful")
                    
                    # Cleanup
                    os.remove(test_file)
                    os.remove('test_output.txt')
                    return True
        
        print("✗ Output file not created correctly")
        return False
        
    except Exception as e:
        print(f"✗ File operation error: {e}")
        return False


def main():
    """Run all tests"""
    print("="*60)
    print("Apache Beam Installation Test")
    print("="*60)
    
    tests = [
        test_imports,
        test_simple_pipeline,
        test_file_operations
    ]
    
    results = []
    for test in tests:
        results.append(test())
        print()
    
    print("="*60)
    print("Test Summary")
    print("="*60)
    
    if all(results):
        print("✓ All tests passed!")
        print("\nYou're ready to run the main exercise:")
        print("  python apache_beam_exercise.py")
        return 0
    else:
        print("✗ Some tests failed")
        print("\nPlease fix the issues above before running the main exercise")
        return 1


if __name__ == '__main__':
    sys.exit(main())
