"""
Download the Telecom Customer Churn dataset from Kaggle.

Source: https://www.kaggle.com/datasets/abhinavthomas/telecom-customer-churn
File: telecom_churn_data.csv (99,999 rows x 226 columns, ~79MB)

Prerequisites:
    pip install kaggle

    # 1. Go to https://www.kaggle.com/settings/account
    # 2. Click "Create New API Token" -> downloads kaggle.json
    # 3. Place kaggle.json in ~/.kaggle/ (Linux/Mac) or %USERPROFILE%\.kaggle\ (Windows)

Usage:
    python download_data.py
"""
import os
import sys

try:
    from kaggle.api.kaggle_api_extended import KaggleApi
except ImportError:
    print("Please install kaggle CLI: pip install kaggle")
    print("Then set up your API token as described above.")
    sys.exit(1)


def main():
    dataset = "abhinavthomas/telecom-customer-churn"
    output_dir = os.path.dirname(os.path.abspath(__file__))

    print(f"Downloading {dataset}...")
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        dataset,
        path=output_dir,
        unzip=True,
    )

    print(f"Done! File saved to: {output_dir}")
    print("Run:  python spark_batch_etl.py  to start the ETL pipeline.")


if __name__ == "__main__":
    main()
