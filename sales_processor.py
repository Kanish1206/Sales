import polars as pl
import pandas as pd

class SalesProcessor:
    def __init__(self, sales_file, master_file):
        self.sales_file = sales_file
        self.master_file = master_file
        self.result_df = None
        self.pivot_df = None

    def process(self):
        # 1. Load Data with the correct engine for .xlsb
        # 'calamine' is the most robust engine for various excel formats
        sales = pl.read_excel(self.sales_file, engine="calamine")
        master = pl.read_excel(self.master_file, engine="calamine")

        # 2. Rename Columns (Ensure names exist to avoid crash)
        rename_dict = {
            'PLI APP': 'PLI APP 1', 'PLI CAT': 'PLI CAT 1',
            'CATE ALL': 'CATE ALL 1', 'PLI HSN': 'PLI HSN 1', 'UQM': 'UQM 1'
        }
        # Only rename columns that actually exist in the file
        existing_cols = [c for c in rename_dict.keys() if c in sales.columns]
        sales = sales.rename({c: rename_dict[c] for c in existing_cols})

        # 3. Join Logic 
        cols_to_bring = ['PLI APP', 'PLI CAT', 'CATE ALL', 'PLI HSN', 'UQM']
        # Filter cols_to_bring to only those that exist in master to prevent errors
        cols_to_bring = [c for c in cols_to_bring if c in master.columns]
        
        df = sales.join(
            master.select(['Product Code'] + cols_to_bring), 
            on='Product Code', 
            how='left'
        )

        # 4. Apply Logic Pipeline
        pravin_mask = pl.col('Customer Name').str.contains('(?i)Pravin Masalewale')
        ic_mask = pl.col('Customer Name').str.contains('(?i)Pravin Sales Division|Aveer Foods Ltd|PRAVIN SALES DIVISION')
        credit_mask = pl.col('Billing Description').str.contains(r'SRN|Credit Rate Diff billing')
        
        self.result_df = df.with_columns(
            pl.when(pravin_mask).then(pl.lit('inter')).otherwise(pl.col('PLI APP')).alias('PLI APP'),
            pl.when(pravin_mask).then(pl.lit('N/A')).otherwise(pl.col('CATE ALL')).alias('CATE ALL'),
            pl.when(ic_mask & credit_mask).then(pl.lit('Credit Note IC'))
            .when(ic_mask).then(pl.lit('Domestic IC'))
            .when(pravin_mask).then(pl.lit('inter'))
            .when(credit_mask).then(pl.lit('Credit Note'))
            .otherwise(pl.lit('Domestic')).alias('Type')
        )

        # 5. Pivot Table
        # Ensure numeric columns are actually numeric before pivoting
        val_cols = ["Billed Qty(KG)", "Taxable Value"]
        self.result_df = self.result_df.with_columns([
            pl.col(c).cast(pl.Float64, strict=False).fill_null(0) for c in val_cols
        ])

        self.pivot_df = self.result_df.pivot(
            values=val_cols,
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type", 
            aggregate_function="sum"
        ).fill_null(0)
        
        return self.result_df, self.pivot_df
