"""Generate sample data for local testing without Git LFS"""

import pandas as pd
import numpy as np
from pathlib import Path

def generate_sample_data():
    """Generate realistic sample datasets for testing."""
    
    output_dir = Path("data/kaggle")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    np.random.seed(42)
    
    # 1. Bank Customer Churn Dataset
    n_customers = 500
    bank_data = {
        'customer_id': range(1, n_customers + 1),
        'age': np.random.randint(18, 80, n_customers),
        'gender': np.random.choice(['M', 'F'], n_customers),
        'credit_score': np.random.randint(300, 850, n_customers),
        'account_balance': np.random.randint(0, 250000, n_customers),
        'products_count': np.random.randint(1, 4, n_customers),
        'tenure_years': np.random.randint(0, 10, n_customers),
        'churn': np.random.choice([0, 1], n_customers, p=[0.8, 0.2])
    }
    
    df_bank = pd.DataFrame(bank_data)
    # Add some data quality issues
    df_bank.loc[10:14, 'age'] = np.nan  # Missing values
    df_bank.loc[20:24, 'credit_score'] = 999  # Invalid values (outside range)
    df_bank.to_csv(output_dir / "Bank Customer Churn Prediction.csv", index=False)
    print(f"✅ Generated: Bank Customer Churn Prediction.csv ({len(df_bank)} records)")
    
    # 2. Cards Data 
    n_cards = 1000
    cards_data = {
        'card_id': [f"CC{str(i).zfill(6)}" for i in range(1, n_cards + 1)],
        'customer_id': np.random.randint(1, n_customers + 1, n_cards),
        'card_type': np.random.choice(['Credit', 'Debit', 'Prepaid'], n_cards),
        'issued_date': pd.date_range('2020-01-01', periods=n_cards, freq='D'),
        'expiry_date': pd.date_range('2026-01-01', periods=n_cards, freq='D'),
        'limit': np.random.choice([1000, 5000, 10000, 25000], n_cards),
        'active': np.random.choice([True, False], n_cards, p=[0.9, 0.1])
    }
    
    df_cards = pd.DataFrame(cards_data)
    # Add data quality issues
    df_cards.loc[5:9, 'card_id'] = None  # Missing card IDs
    df_cards.loc[30:34, 'limit'] = -1000  # Invalid negative limits
    df_cards.to_csv(output_dir / "cards_data.csv", index=False)
    print(f"✅ Generated: cards_data.csv ({len(df_cards)} records)")
    
    # 3. Telco Customer Churn
    n_telco = 600
    telco_data = {
        'customer_id': [f"TEL{str(i).zfill(6)}" for i in range(1, n_telco + 1)],
        'tenure_months': np.random.randint(0, 72, n_telco),
        'monthly_charge': np.random.uniform(20, 150, n_telco),
        'total_charges': np.random.uniform(20, 8000, n_telco),
        'internet_service': np.random.choice(['Fiber', 'DSL', 'None'], n_telco),
        'online_security': np.random.choice(['Yes', 'No'], n_telco),
        'churn': np.random.choice(['Yes', 'No'], n_telco, p=[0.26, 0.74])
    }
    
    df_telco = pd.DataFrame(telco_data)
    # Add data quality issues
    df_telco.loc[40:44, 'customer_id'] = ''  # Empty customer IDs
    df_telco.loc[50:54, 'monthly_charge'] = -50  # Negative charges
    df_telco.to_csv(output_dir / "telco_customer_churn.csv", index=False)
    print(f"✅ Generated: telco_customer_churn.csv ({len(df_telco)} records)")
    
    # 4. Users Data
    n_users = 400
    users_data = {
        'user_id': range(1, n_users + 1),
        'name': [f"User_{i}" for i in range(1, n_users + 1)],
        'email': [f"user{i}@example.com" for i in range(1, n_users + 1)],
        'signup_date': pd.date_range('2022-01-01', periods=n_users, freq='D'),
        'role': np.random.choice(['Admin', 'User', 'Guest'], n_users),
        'active': np.random.choice([True, False], n_users, p=[0.85, 0.15]),
        'monthly_spend': np.random.uniform(0, 500, n_users)
    }
    
    df_users = pd.DataFrame(users_data)
    # Add data quality issues
    df_users.loc[15:19, 'email'] = 'invalid-email'  # Invalid emails
    df_users.loc[60:64, 'monthly_spend'] = np.nan  # Missing values
    df_users.to_csv(output_dir / "users_data.csv", index=False)
    print(f"✅ Generated: users_data.csv ({len(df_users)} records)")
    
    print("\n✅ All sample datasets generated successfully!")
    print(f"\nDatasets location: {output_dir.absolute()}")
    print(f"Total files created: 4")

if __name__ == "__main__":
    generate_sample_data()
