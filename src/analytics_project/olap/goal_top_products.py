"""Module 6: OLAP Goal Script (uses cubed results).

File: src/analytics_project/olap/goal_top_product.py.

Module: analytics_project.olap.goal_top_product.py

This script uses our precomputed cubed data set to get the information
we need to answer a specific business goal.

GOAL: Analyze sales data to determine

ACTION: This can help inform inventory decisions, optimize promotions,
and understand purchasing patterns on different days.

PROCESS:
Group transactions by the day of the week and product.
Sum SaleAmount for each product on each day.
Identify the top product for each day based on total revenue.

DayOfWeek,product_id,customer_id,sale_amountd_sum,sale_amount_mean,sale_id_count,sale_ids
Friday,101,1001,6344.96,6344.96,1,[582]
"""

import pathlib
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

from analytics_project.utils_logger import logger

# ----------------------------
# PATH SETUP (must be global)
# ----------------------------
THIS_DIR = pathlib.Path(__file__).resolve().parent
PACKAGE_DIR = THIS_DIR.parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent

# Old:
# DATA_DIR = PROJECT_ROOT / "data"
# RESULTS_DIR = DATA_DIR / "results"

# New:
RESULTS_DIR = THIS_DIR
RESULTS_DIR.mkdir(exist_ok=True)

DATA_DIR = PROJECT_ROOT / "data"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
DB_PATH = WAREHOUSE_DIR / "smart_sales.db"


# Recommended - log paths and key directories for debugging
logger.info(f"THIS_DIR:       {THIS_DIR}")
logger.info(f"PACKAGE_DIR:    {PACKAGE_DIR}")
logger.info(f"SRC_DIR:        {SRC_DIR}")
logger.info(f"PROJECT_ROOT:   {PROJECT_ROOT}")
logger.info(f"DATA_DIR:       {DATA_DIR}")
logger.info(f"WAREHOUSE_DIR:  {WAREHOUSE_DIR}")
logger.info(f"DB_PATH:        {DB_PATH}")
logger.info(f"RESULTS_DIR:    {RESULTS_DIR}")


# ---------------------------------------------------------
# LOAD DATA FROM WAREHOUSE
# ---------------------------------------------------------
def load_dw_sales() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT
        s.transaction_id,
        s.sale_amount,
        s.sale_date,
        s.product_id,
        p.product_name,
        p.category,
        c.region
    FROM sale AS s
    JOIN product AS p ON s.product_id = p.product_id
    JOIN customer AS c ON s.customer_id = c.customer_id
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert date
    df["sale_date"] = pd.to_datetime(df["sale_date"])

    # FIX: Ensure sale_amount is numeric
    df["sale_amount"] = pd.to_numeric(df["sale_amount"], errors="coerce").fillna(0)

    # Extract year & month
    df["year"] = df["sale_date"].dt.year
    df["month"] = df["sale_date"].dt.month
    df["day_of_week"] = df["sale_date"].dt.day_name()

    logger.info(f"Loaded DW data: {len(df)} rows")
    return df


# ----------------------------
# TOP PRODUCTS (OVERALL)
# ----------------------------
def compute_top_products(df: pd.DataFrame, top_n=10) -> pd.DataFrame:
    top = (
        df.groupby(["product_id", "product_name", "category"])
        .agg(total_revenue=("sale_amount", "sum"), transactions=("transaction_id", "count"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
        .head(top_n)
    )

    out = RESULTS_DIR / "top_products_overall.csv"
    top.to_csv(out, index=False)
    logger.info(f"Saved top products table: {out}")

    return top


def plot_top_products(df: pd.DataFrame):
    plt.figure(figsize=(12, 6))
    plt.bar(df["product_name"], df["total_revenue"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Top-Selling Products by Total Revenue")
    plt.xlabel("Product")
    plt.ylabel("Revenue")
    plt.tight_layout()

    out = RESULTS_DIR / "top_products_overall.png"
    plt.savefig(out)
    logger.info(f"Bar chart saved: {out}")
    plt.close()


def plot_top_products_region(region_df: pd.DataFrame, region: str) -> None:
    """Generate Top 15 and Top 5 charts for a region."""
    if region_df.empty:
        logger.warning(f"No data to plot for region: {region}")
        return

    # ---- TOP 15 ----
    top15 = region_df.head(15)

    plt.figure(figsize=(12, 6))
    plt.bar(top15["product_name"], top15["total_revenue"])
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Top 15 Products in {region.capitalize()} Region")
    plt.xlabel("Product")
    plt.ylabel("Revenue")
    plt.tight_layout()

    out15 = RESULTS_DIR / f"top15_products_region_{region}.png"
    plt.savefig(out15)
    logger.info(f"Top 15 chart saved: {out15}")
    plt.close()

    # ---- TOP 5 ----
    top5 = region_df.head(5)

    plt.figure(figsize=(10, 5))
    plt.bar(top5["product_name"], top5["total_revenue"])
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Top 5 Products in {region.capitalize()} Region")
    plt.xlabel("Product")
    plt.ylabel


def compute_lowest_products(df: pd.DataFrame, bottom_n=10) -> pd.DataFrame:
    lowest = (
        df.groupby(["product_id", "product_name", "category"])
        .agg(total_revenue=("sale_amount", "sum"), transactions=("transaction_id", "count"))
        .reset_index()
        .sort_values("total_revenue", ascending=True)
        .head(bottom_n)
    )

    out = RESULTS_DIR / "lowest_products_overall.csv"
    lowest.to_csv(out, index=False)
    logger.info(f"Saved lowest products table: {out}")

    return lowest


def plot_lowest_products(df: pd.DataFrame):
    plt.figure(figsize=(12, 6))
    plt.bar(df["product_name"], df["total_revenue"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Lowest-Selling Products by Total Revenue")
    plt.xlabel("Product")
    plt.ylabel("Revenue")
    plt.tight_layout()

    out = RESULTS_DIR / "lowest_products_overall.png"
    plt.savefig(out)
    logger.info(f"Lowest products bar chart saved: {out}")
    plt.close()


# ----------------------------
# SLICE: BY CATEGORY
# ----------------------------
def slice_by_category(df: pd.DataFrame, category: str):
    sliced = df[df["category"] == category]

    agg = (
        sliced.groupby(["product_id", "product_name"])
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    out = RESULTS_DIR / f"slice_category_{category}.csv"
    agg.to_csv(out, index=False)
    logger.info(f"Saved category slice: {out}")

    return agg


def slice_by_region(df: pd.DataFrame, region: str):
    sliced = df[df["region"] == region]

    agg = (
        sliced.groupby(["product_id", "product_name"])
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    out = RESULTS_DIR / f"slice_region_{region}.csv"
    agg.to_csv(out, index=False)
    logger.info(f"Saved region slice: {out}")

    return agg


# ----------------------------
# DICE: PRODUCT × MONTH
# ----------------------------
def dice_product_by_month(df: pd.DataFrame, product_ids):
    diced = df[df["product_id"].isin(product_ids)]

    agg = (
        diced.groupby(["product_id", "product_name", "month"])
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
    )

    out = RESULTS_DIR / "dice_product_month.csv"
    agg.to_csv(out, index=False)
    logger.info(f"Saved diced results: {out}")

    return agg


def dice_region_by_product(df: pd.DataFrame):
    agg = (
        df.groupby(["region", "product_id", "product_name"])
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
    )

    out = RESULTS_DIR / "dice_region_product.csv"
    agg.to_csv(out, index=False)
    logger.info(f"Saved region-product dice results: {out}")

    return agg


def plot_dice_region_product(diced_df: pd.DataFrame, top_n=5) -> None:
    """Stacked bar: top N products by total revenue across all regions."""

    if diced_df.empty:
        logger.warning("No data to plot for region × product dice.")
        return

    # ---- LIMIT TO TOP N PRODUCTS ----
    top_products = (
        diced_df.groupby("product_name")["total_revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .index
    )

    diced_df = diced_df[diced_df["product_name"].isin(top_products)]

    # Pivot for stacked bar
    pivot = diced_df.pivot_table(
        index="region",
        columns="product_name",
        values="total_revenue",
        aggfunc="sum",
        fill_value=0,
    )

    plt.figure(figsize=(12, 7))
    pivot.plot(kind="bar", stacked=True, ax=plt.gca())
    plt.title(f"Revenue by Region (Top {top_n} Products)")
    plt.xlabel("Region")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=0)
    plt.tight_layout()

    out = RESULTS_DIR / f"dice_region_product_top{top_n}.png"
    plt.savefig(out)
    logger.info(f"Region × product dice chart saved: {out}")
    plt.close()


# ----------------------------
# DRILLDOWN: YEAR → MONTH
# ----------------------------
def drilldown_year_month(df: pd.DataFrame):
    drill = (
        df.groupby(["year", "month"])
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
        .sort_values(["year", "month"])
    )

    out = RESULTS_DIR / "drilldown_year_month.csv"
    drill.to_csv(out, index=False)
    logger.info(f"Saved drilldown report: {out}")

    return drill


def drilldown_region_month(df: pd.DataFrame):
    drill = (
        df.groupby(["region", "year", "month"])
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
        .sort_values(["region", "year", "month"])
    )

    out = RESULTS_DIR / "drilldown_region_month.csv"
    drill.to_csv(out, index=False)
    logger.info(f"Saved region-month drilldown: {out}")

    return drill


def plot_region_month_trend(drill_df: pd.DataFrame) -> None:
    """Line chart: monthly revenue trend per region."""
    if drill_df.empty:
        logger.warning("No data to plot for region-month drilldown.")
        return

    # Build a nice period label like "2024-03"
    drill_df = drill_df.copy()
    drill_df["period"] = (
        drill_df["year"].astype(str) + "-" + drill_df["month"].astype(str).str.zfill(2)
    )

    pivot = drill_df.pivot_table(
        index="period",
        columns="region",
        values="total_revenue",
        aggfunc="sum",
        fill_value=0,
    )

    pivot = pivot.sort_index()

    plt.figure(figsize=(12, 6))
    for col in pivot.columns:
        plt.plot(pivot.index, pivot[col], marker="o", label=col)

    plt.title("Monthly Revenue by Region")
    plt.xlabel("Year-Month")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Region")
    plt.tight_layout()

    out = RESULTS_DIR / "region_month_trend.png"
    plt.savefig(out)
    logger.info(f"Region-month trend chart saved: {out}")
    plt.close()


def compare_top_products_across_regions(df: pd.DataFrame):
    """
    Returns the top-selling product (by revenue) for each region.
    """
    top_by_region = (
        df.groupby(["region", "product_name"])
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
    )

    # For each region, pick the product with the max revenue
    idx = top_by_region.groupby("region")["total_revenue"].idxmax()
    winners = top_by_region.loc[idx].reset_index(drop=True)

    out_csv = RESULTS_DIR / "top_product_per_region.csv"
    winners.to_csv(out_csv, index=False)
    logger.info(f"Saved top product per region table: {out_csv}")

    return winners


def plot_top_products_across_regions(winners: pd.DataFrame):
    """
    Bar chart showing the top product in each region.
    """
    plt.figure(figsize=(10, 6))
    plt.bar(winners["region"], winners["total_revenue"], color="skyblue")

    # Show labels on bars
    for i, row in winners.iterrows():
        plt.text(
            i, row["total_revenue"] + 200, row["product_name"], ha="center", rotation=45, fontsize=8
        )

    plt.title("Top Product in Each Region")
    plt.xlabel("Region")
    plt.ylabel("Revenue")
    plt.tight_layout()

    out = RESULTS_DIR / "top_product_per_region.png"
    plt.savefig(out)
    logger.info(f"Top product across regions chart saved: {out}")
    plt.close()


# ----------------------------
# MAIN
# ----------------------------
def main():
    logger.info("Starting DW-based Top Product Analysis...")

    df = load_dw_sales()

    # Region-Level Analysis
    revenue_per_region(df)
    transactions_per_region(df)

    # Top products (overall)
    top = compute_top_products(df)
    plot_top_products(top)

    # Lowest products (overall)
    lowest = compute_lowest_products(df)
    plot_lowest_products(lowest)

    # Category slice
    try:
        slice_by_category(df, "Electronics")
    except Exception:
        logger.warning("Category slice failed (category may not exist).")

    # Dicing: Top 3 products by month
    top_ids = top["product_id"].head(3).tolist()
    dice_product_by_month(df, top_ids)

    # Drilldown year → month
    drilldown_year_month(df)

    # 2) Dice region × product + chart (all regions together)
    diced_region_product = dice_region_by_product(df)
    plot_dice_region_product(diced_region_product, top_n=5)

    # 3) Drilldown region → month + chart (all regions together)
    drill_region_month = drilldown_region_month(df)
    plot_region_month_trend(drill_region_month)

    # === Run analysis for each region ===
    REGIONS = ["west", "east", "central", "north", "south", "south-west"]

    for region in REGIONS:
        logger.info(f"\n=== Processing Region: {region} ===")

        # Get aggregated slice for this region
        region_slice = slice_by_region(df, region)

        # Only plot if there is data
        if not region_slice.empty:
            plot_top_products_region(region_slice, region)
        else:
            logger.warning(f"No data to plot for region: {region}")

    # --- Compare top product across regions ---
    winners = compare_top_products_across_regions(df)
    plot_top_products_across_regions(winners)


def revenue_per_region(df: pd.DataFrame):
    """Compute total revenue per region and save CSV + bar chart."""
    agg = (
        df.groupby("region")
        .agg(total_revenue=("sale_amount", "sum"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    # Save CSV
    out_csv = RESULTS_DIR / "revenue_per_region.csv"
    agg.to_csv(out_csv, index=False)
    logger.info(f"Saved revenue-per-region table: {out_csv}")

    # Plot
    plt.figure(figsize=(10, 6))
    plt.bar(agg["region"], agg["total_revenue"])
    plt.title("Total Revenue per Region")
    plt.xlabel("Region")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_png = RESULTS_DIR / "revenue_per_region.png"
    plt.savefig(out_png)
    logger.info(f"Revenue-per-region chart saved: {out_png}")
    plt.close()

    return agg


def transactions_per_region(df: pd.DataFrame):
    """Compute number of transactions per region and save CSV + bar chart."""
    agg = (
        df.groupby("region")
        .agg(transaction_count=("transaction_id", "count"))
        .reset_index()
        .sort_values("transaction_count", ascending=False)
    )

    # Save CSV
    out_csv = RESULTS_DIR / "transactions_per_region.csv"
    agg.to_csv(out_csv, index=False)
    logger.info(f"Saved transactions-per-region table: {out_csv}")

    # Plot
    plt.figure(figsize=(10, 6))
    plt.bar(agg["region"], agg["transaction_count"])
    plt.title("Number of Transactions per Region")
    plt.xlabel("Region")
    plt.ylabel("Transaction Count")
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_png = RESULTS_DIR / "transactions_per_region.png"
    plt.savefig(out_png)
    logger.info(f"Transactions-per-region chart saved: {out_png}")
    plt.close()

    return agg


if __name__ == "__main__":
    main()
