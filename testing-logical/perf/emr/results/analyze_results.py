#!/usr/bin/env python3
"""
Hudi Benchmark Results Analyzer
Analyzes performance metrics and generates comparison reports

Usage:
  python3 analyze_results.py <metrics_csv_file>
"""

import sys
import pandas as pd

def load_metrics(metrics_file):
    """Load metrics CSV file"""
    try:
        df = pd.read_csv(metrics_file)
        print(f"✅ Loaded {len(df)} metric entries from {metrics_file}")
        return df
    except Exception as e:
        print(f"❌ Error loading metrics: {e}")
        sys.exit(1)

def validate_metrics(df):
    """Validate metrics data"""
    required_cols = ['config_type', 'table_type', 'scenario', 'hudi_version', 'operation', 'latency_seconds']

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing required columns: {missing_cols}")
        sys.exit(1)

    # Check for missing values
    missing_data = df[required_cols].isnull().sum()
    if missing_data.any():
        print("⚠️  Warning: Missing values detected:")
        print(missing_data[missing_data > 0])

    # Expected: 12 tests × 5 operations = 60 entries
    expected_count = 60
    if len(df) != expected_count:
        print(f"⚠️  Warning: Expected {expected_count} entries, found {len(df)}")

    print("✅ Validation passed")

def generate_summary(df):
    """Generate summary statistics"""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)

    # Group by major dimensions
    summary = df.groupby(['config_type', 'table_type', 'scenario', 'hudi_version'])['latency_seconds'].agg([
        ('total_latency', 'sum'),
        ('avg_latency', 'mean'),
        ('operations', 'count')
    ]).round(2)

    print(summary)

def compare_scenarios(df):
    """Compare baseline vs experimental scenarios"""
    print("\n" + "="*80)
    print("BASELINE vs EXPERIMENTAL COMPARISON")
    print("="*80)

    # Pivot to compare scenarios side by side
    comparison = pd.pivot_table(
        df,
        values='latency_seconds',
        index=['config_type', 'table_type', 'operation'],
        columns='scenario',
        aggfunc='sum'
    )

    if 'baseline' in comparison.columns and 'experimental' in comparison.columns:
        # Calculate delta
        comparison['delta_seconds'] = comparison['experimental'] - comparison['baseline']
        comparison['delta_percent'] = (comparison['delta_seconds'] / comparison['baseline']) * 100

        # Round for readability
        comparison = comparison.round(2)

        print(comparison)

        # Identify significant changes
        print("\n" + "="*80)
        print("SIGNIFICANT CHANGES (> 5% difference)")
        print("="*80)

        significant = comparison[abs(comparison['delta_percent']) > 5.0]
        if not significant.empty:
            print(significant[['baseline', 'experimental', 'delta_seconds', 'delta_percent']])
        else:
            print("No significant performance differences detected")

    return comparison

def compare_versions(df):
    """Compare Hudi versions"""
    print("\n" + "="*80)
    print("HUDI VERSION COMPARISON")
    print("="*80)

    # Group by version
    version_summary = df.groupby(['hudi_version', 'config_type', 'table_type'])['latency_seconds'].agg([
        ('total_latency', 'sum'),
        ('operations', 'count')
    ]).round(2)

    print(version_summary)

def analyze_by_config(df):
    """Analyze Config A vs Config B"""
    print("\n" + "="*80)
    print("CONFIG A (with logical types) vs CONFIG B (without)")
    print("="*80)

    config_summary = df.groupby(['config_type', 'scenario', 'operation'])['latency_seconds'].agg([
        ('avg_latency', 'mean'),
        ('total_latency', 'sum')
    ]).round(2)

    print(config_summary)

    # Compare Config A vs B for each operation in experimental scenario
    print("\n" + "="*80)
    print("OVERHEAD ANALYSIS: Config A vs Config B (Experimental)")
    print("="*80)

    experimental_df = df[df['scenario'] == 'experimental']
    config_pivot = pd.pivot_table(
        experimental_df,
        values='latency_seconds',
        index=['table_type', 'operation'],
        columns='config_type',
        aggfunc='mean'
    )

    if 'config_a' in config_pivot.columns and 'config_b' in config_pivot.columns:
        config_pivot['overhead_seconds'] = config_pivot['config_a'] - config_pivot['config_b']
        config_pivot['overhead_percent'] = (config_pivot['overhead_seconds'] / config_pivot['config_b']) * 100
        config_pivot = config_pivot.round(2)

        print(config_pivot)

        # Check if overhead is minimal
        avg_overhead = config_pivot['overhead_percent'].mean()
        print(f"\nAverage overhead from logical types: {avg_overhead:.2f}%")

        if abs(avg_overhead) < 5.0:
            print("✅ Overhead is minimal (<5%), logical type fix has no significant performance impact")
        else:
            print("⚠️  Overhead detected, review results for specific operations")

def save_detailed_report(df, comparison, output_file):
    """Save detailed analysis to file"""
    try:
        # Save comparison to CSV
        comparison.to_csv(output_file)
        print(f"\n✅ Detailed report saved to: {output_file}")
    except Exception as e:
        print(f"⚠️  Warning: Could not save report: {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 analyze_results.py <metrics_csv_file>")
        sys.exit(1)

    metrics_file = sys.argv[1]

    print("="*80)
    print("HUDI PERFORMANCE BENCHMARK ANALYSIS")
    print("="*80)
    print(f"Input file: {metrics_file}\n")

    # Load and validate data
    df = load_metrics(metrics_file)
    validate_metrics(df)

    # Generate analyses
    generate_summary(df)
    comparison = compare_scenarios(df)
    compare_versions(df)
    analyze_by_config(df)

    # Save detailed report
    output_file = metrics_file.replace('.csv', '_analysis.csv')
    save_detailed_report(df, comparison, output_file)

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
