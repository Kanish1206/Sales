# sales_processor.py

import pandas as pd
import polars as pl
import re

class SalesProcessor:
    def __init__(self, sales_file, master_file):
        self.sales_file = sales_file
        self.master_file = master_file

    # --------------------------------------------------
    # FILE LOADERS (STREAMLIT SAFE)
    # --------------------------------------------------
    def _load_sales(self) -> pl.DataFrame:
        if self.sales_file.name.endswith(".xlsb"):
            pdf = pd.read_excel(self.sales_file, engine="pyxlsb")
        else:
            pdf = pd.read_excel(self.sales_file,engine="openpyxl")

        return pl.from_pandas(pdf)

    def _load_master(self) -> pl.DataFrame:
        pdf = pd.read_excel(self.master_file)
        return pl.from_pandas(pdf)

    # --------------------------------------------------
    # CORE PROCESS
    # --------------------------------------------------
    def process(self):
        sales = self._load_sales()
        master = self._load_master()

        # ---- Column Cleanup ----
        sales = sales.with_columns([
            pl.col(pl.Utf8).str.strip_chars()
        ])

        master = master.with_columns([
            pl.col(pl.Utf8).str.strip_chars()
        ])

        # ---- Join Master ----
        columns_to_bring = ["PLI APP", "PLI CAT", "CATE ALL", "PLI HSN", "UQM"]

        sales = sales.join(
            master.select(["Product Code"] + columns_to_bring),
            on="Product Code",
            how="left"
        )

        # ---- Customer Overrides ----
        def cust_match(col):
            return pl.col("Customer Name").str.contains(col, literal=False)

        sales = sales.with_columns([
            pl.when(cust_match("(?i)Pravin Masalewale"))
              .then(pl.lit("inter"))
              .otherwise(pl.col("PLI APP"))
              .alias("PLI APP"),

            pl.when(cust_match("(?i)Pravin Masalewale"))
              .then(pl.lit("inter"))
              .otherwise(pl.col("PLI CAT"))
              .alias("PLI CAT"),

            pl.when(cust_match("(?i)Pravin Masalewale"))
              .then(pl.lit("N/A"))
              .otherwise(pl.col("CATE ALL"))
              .alias("CATE ALL"),

            pl.when(cust_match("(?i)Pravin Masalewale"))
              .then(pl.lit("N/A"))
              .otherwise(pl.col("PLI HSN"))
              .alias("PLI HSN"),

            pl.when(cust_match("(?i)Pravin Masalewale"))
              .then(pl.lit("N/A"))
              .otherwise(pl.col("UQM"))
              .alias("UQM")
        ])

        # ---- TYPE LOGIC ----
        sales = sales.with_columns(
            pl.when(pl.col("Billing Description").str.contains("SRN|Credit Rate Diff billing"))
              .then(pl.lit("Credit Note"))

            .when(pl.col("Billing Description").str.contains("Export  Direct Billing"))
              .then(pl.lit("Export"))

            .when(
                cust_match("(?i)Pravin Sales Division|Aveer Foods Ltd") &
                pl.col("Billing Description").str.contains("SRN|Credit Rate Diff billing")
            )
            .then(pl.lit("Credit Note IC"))

            .when(cust_match("(?i)Pravin Sales Division|Aveer Foods Ltd"))
            .then(pl.lit("Domestic IC"))

            .otherwise(pl.lit("Domestic"))
            .alias("Type")
        )

        # ---- Numeric Safety ----
        sales = sales.with_columns([
            pl.col("Billed Qty(KG)").cast(pl.Float64, strict=False),
            pl.col("Taxable Value").cast(pl.Float64, strict=False)
        ])

        # ---- Pivot ----
        pivot = sales.pivot(
            values=["Billed Qty(KG)", "Taxable Value"],
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type",
            aggregate_function="sum"
        )

        # ---- Column Ordering ----
        ordered_types = [
            "Domestic",
            "Domestic IC",
            "inter",
            "Credit Note",
            "Credit Note IC",
            "Export"
        ]

        ordered_cols = []
        for t in ordered_types:
            for base in ["Billed Qty(KG)", "Taxable Value"]:
                col = f"{base}_{t}"
                if col in pivot.columns:
                    ordered_cols.append(col)

        pivot = pivot.select(
            ["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"] + ordered_cols
        )

        return sales, pivot

