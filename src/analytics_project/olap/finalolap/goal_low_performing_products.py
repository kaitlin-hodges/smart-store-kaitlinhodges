"""
P7 - Custom BI Project
Goal: Determine low-performing products by region and evaluate whether discounts hurt or help sales.
"""

import pathlib
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from analytics_project.utils_logger import logger

# ---------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------

THIS_DIR = pathlib.Path(__file__).resolve().parent
RESULTS_DIR = THIS_DIR / "Results"
RESULTS_DIR.mkdir(exist_ok=True)

PROJECT_ROOT = THIS_DIR.parents[3]

# *** YOUR REAL DATABASE ***
DW_PATH = PROJECT_ROOT / "data" / "warehouse" / "smart_sales.db"

logger.info(f"THIS_DIR:       {THIS_DIR}")
logger.info(f"RESULTS_DIR:    {RESULTS_DIR}")
logger.info(f"PROJECT_ROOT:   {PROJECT_ROOT}")
logger.info(f"DW_PATH:        {DW_PATH}")


# ---------------------------------------------------
# LOAD DATA FROM WAREHOUSE
# ---------------------------------------------------
def load_sales():
    query = """
    SELECT
        s.transaction_id      AS transaction_id,
        s.sale_amount         AS sale_amount,
        s.sale_date           AS sale_date,
        s.product_id          AS product_id,
        s.discount_percentage AS discount_percentage,
        p.product_name        AS product_name,
        p.category            AS category,
        c.region              AS region
    FROM sale AS s
    JOIN product AS p
        ON s.product_id = p.product_id
    JOIN customer AS c
        ON s.customer_id = c.customer_id;
    """

    with sqlite3.connect(DW_PATH) as conn:
        df = pd.read_sql_query(query, conn)

    # fix datatypes
    df["sale_amount"] = pd.to_numeric(df["sale_amount"], errors="coerce").fillna(0)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")

    # 🔑 ensure we have a 'discount_pct' column, derived from discount_percentage if present
    if "discount_pct" not in df.columns:
        if "discount_percentage" in df.columns:
            df["discount_pct"] = df["discount_percentage"]
        else:
            df["discount_pct"] = 0

    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)

    return df


# ---------------------------------------------------
# LOW PERFORMERS (BOTTOM 5 PER REGION)
# ---------------------------------------------------
def find_low_performers(df):
    agg = df.groupby(["region", "product_id", "product_name"], as_index=False).agg(
        total_revenue=("sale_amount", "sum"),
        avg_discount=("discount_pct", "mean"),
        transactions=("transaction_id", "count"),
    )

    low = (
        agg.sort_values(["region", "total_revenue"], ascending=True)
        .groupby("region")
        .head(5)
        .reset_index(drop=True)
    )

    out_csv = RESULTS_DIR / "low_performers_by_region.csv"
    low.to_csv(out_csv, index=False)
    logger.info(f"Saved low performers → {out_csv}")

    return low


# ---------------------------------------------------
# VISUALS
# ---------------------------------------------------
def plot_low_performers(low_df):
    for region in low_df["region"].unique():
        region_df = low_df[low_df["region"] == region].sort_values("total_revenue")

        plt.figure(figsize=(12, 6))
        plt.barh(region_df["product_name"], region_df["total_revenue"])
        plt.title(f"Bottom 5 Products — {region}")
        plt.xlabel("Total Revenue")
        plt.ylabel("Product")
        plt.tight_layout()

        out_file = RESULTS_DIR / f"low_perf_region_{region}.png"
        plt.savefig(out_file)
        plt.close()

        logger.info(f"Saved chart for region {region} → {out_file}")


# ---------------------------------------------------
# DISCOUNT IMPACT
# ---------------------------------------------------
def analyze_discounts(low_df):
    plt.figure(figsize=(10, 6))
    plt.scatter(low_df["avg_discount"], low_df["total_revenue"])
    plt.xlabel("Average Discount (%)")
    plt.ylabel("Total Revenue")
    plt.title("Discount Impact on Low-Performing Products")
    plt.tight_layout()

    out_file = RESULTS_DIR / "discount_impact_low_performers.png"
    plt.savefig(out_file)
    plt.close()

    logger.info(f"Saved discount impact chart → {out_file}")


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
def main():
    logger.info("Starting Low Performing Product Analysis...")

    df = load_sales()
    low = find_low_performers(df)

    plot_low_performers(low)
    analyze_discounts(low)

    logger.info("Analysis complete.")


if __name__ == "__main__":
    main()
