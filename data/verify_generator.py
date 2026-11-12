"""
Quick verification script - checks all generated CSV files are correct.
Run this after generator.py to confirm everything looks right.
"""
import pandas as pd
import numpy as np
import os

RAW = r'data/raw'

# ── List all files ───────────────────────────────────────────
files = sorted(os.listdir(RAW))
print('=' * 65)
print('FILES IN data/raw/')
print('=' * 65)
for f in files:
    path = os.path.join(RAW, f)
    size = os.path.getsize(path) / 1024
    print(f'  {f:48s}  {size:7.1f} KB')

PASS = "[PASS]"
FAIL = "[FAIL]"

results = []

def check_scenario(date_str, label, expected):
    """
    expected is a dict of things we expect to be true:
      - min_customers / max_customers  : row count range
      - customer_id_type               : 'int' or 'str'
      - revenue_null_pct_min           : minimum % of revenue nulls
      - duplicate_ids                  : True/False
      - orders_missing                 : True if file should not exist
    """
    print(f'\n{"-"*65}')
    print(f'  Scenario: {label}  [{date_str}]')
    print(f'{"-"*65}')

    cust_path = os.path.join(RAW, f'raw_customers_{date_str}.csv')
    ord_path  = os.path.join(RAW, f'raw_orders_{date_str}.csv')
    passed    = True

    # ── Customers file ────────────────────────────────────────
    if not os.path.exists(cust_path):
        print(f'  {FAIL}  Customers file missing!')
        results.append((label, 'FAIL'))
        return

    c = pd.read_csv(cust_path)
    print(f'  Customers : {len(c):,} rows')

    # Row count check
    mn = expected.get('min_customers', 0)
    mx = expected.get('max_customers', 999_999)
    if mn <= len(c) <= mx:
        print(f'  {PASS}  Row count in expected range [{mn:,} – {mx:,}]')
    else:
        print(f'  {FAIL}  Row count {len(c):,} NOT in expected range [{mn:,} – {mx:,}]')
        passed = False

    # customer_id dtype
    cid_sample = c['customer_id'].iloc[0]
    actual_type = 'str' if isinstance(cid_sample, str) else 'int'
    exp_type    = expected.get('customer_id_type', 'int')
    if actual_type == exp_type:
        print(f'  {PASS}  customer_id dtype = {actual_type}  (sample: {cid_sample})')
    else:
        print(f'  {FAIL}  customer_id dtype is {actual_type}, expected {exp_type}  (sample: {cid_sample})')
        passed = False

    # Duplicate check
    dup_count = c.duplicated(subset=['customer_id']).sum()
    exp_dups  = expected.get('duplicate_ids', False)
    if exp_dups:
        if dup_count > 0:
            print(f'  {PASS}  Duplicates found: {dup_count} rows (expected)')
        else:
            print(f'  {FAIL}  Expected duplicates but found none')
            passed = False
    else:
        if dup_count == 0:
            print(f'  {PASS}  No duplicate customer_ids')
        else:
            print(f'  {FAIL}  Unexpected duplicates: {dup_count}')
            passed = False

    # Null count
    total_nulls = c.isnull().sum().sum()
    print(f'  Info      : {total_nulls} null values in customers file')

    # ── Orders file ────────────────────────────────────────────
    orders_should_be_missing = expected.get('orders_missing', False)
    if orders_should_be_missing:
        if not os.path.exists(ord_path):
            print(f'  {PASS}  Orders file correctly absent (missing_file scenario)')
        else:
            print(f'  {FAIL}  Orders file exists but should be missing!')
            passed = False
    else:
        if not os.path.exists(ord_path):
            print(f'  {FAIL}  Orders file missing (unexpected)')
            passed = False
        else:
            o = pd.read_csv(ord_path)
            print(f'  Orders    : {len(o):,} rows')

            # Revenue null check
            rev_null_pct = o['revenue'].isnull().mean() * 100
            exp_null_min = expected.get('revenue_null_pct_min', 0)
            if rev_null_pct >= exp_null_min:
                status = PASS if exp_null_min > 0 else PASS
                print(f'  {status}  Revenue null %: {rev_null_pct:.1f}%  (expected >= {exp_null_min}%)')
            else:
                print(f'  {FAIL}  Revenue null %: {rev_null_pct:.1f}%  (expected >= {exp_null_min}%)')
                passed = False

            # Referential integrity: every order's customer_id exists in customers
            orphans = (~o['customer_id'].isin(c['customer_id'])).sum()
            if orphans == 0:
                print(f'  {PASS}  Referential integrity OK — no orphan orders')
            else:
                print(f'  {FAIL}  {orphans} orders reference non-existent customer_ids!')
                passed = False

    results.append((label, 'PASS' if passed else 'FAIL'))


# ── Run all checks ───────────────────────────────────────────
check_scenario('20261112', 'NORMAL (weekday)',
    expected={
        'min_customers': 9_500, 'max_customers': 11_000,
        'customer_id_type': 'int',
        'duplicate_ids': False,
        'revenue_null_pct_min': 0,
    }
)

check_scenario('20261111', 'ROW DROP',
    expected={
        'min_customers': 1_000, 'max_customers': 3_000,
        'customer_id_type': 'int',
        'duplicate_ids': False,
        'revenue_null_pct_min': 0,
    }
)

check_scenario('20261109', 'SCHEMA DRIFT',
    expected={
        'min_customers': 9_500, 'max_customers': 11_000,
        'customer_id_type': 'str',   # <-- customer_id should now be "CUST_123"
        'duplicate_ids': False,
        'revenue_null_pct_min': 0,
    }
)

check_scenario('20261108', 'NULL SPIKE (weekend)',
    expected={
        'min_customers': 5_500, 'max_customers': 7_000,
        'customer_id_type': 'int',
        'duplicate_ids': False,
        'revenue_null_pct_min': 55.0,  # expect at least 55% nulls in revenue
    }
)

check_scenario('20261107', 'DUPLICATE (weekend)',
    expected={
        'min_customers': 7_000, 'max_customers': 9_000,
        'customer_id_type': 'int',
        'duplicate_ids': True,         # <-- expect duplicates
        'revenue_null_pct_min': 0,
    }
)

check_scenario('20261106', 'MISSING FILE',
    expected={
        'min_customers': 9_500, 'max_customers': 11_000,
        'customer_id_type': 'int',
        'duplicate_ids': False,
        'orders_missing': True,        # <-- orders file should not exist
    }
)

# ── Summary ──────────────────────────────────────────────────
print(f'\n{"="*65}')
print('VERIFICATION SUMMARY')
print(f'{"="*65}')
all_pass = True
for label, status in results:
    icon = "[PASS]" if status == "PASS" else "[FAIL]"
    print(f'  {icon}  {label}')
    if status != "PASS":
        all_pass = False

print()
if all_pass:
    print('  All checks passed. Generator is working correctly.')
else:
    print('  Some checks failed. Review output above.')
print(f'{"="*65}')
