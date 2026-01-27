import polars as pl
import pandas as pd

class SalesProcessor:
    def __init__(self, sales_file, master_file):
        self.sales_file = sales_file
        self.master_file = master_file
        self.result_df = None
        self.pivot_df = None

    def process(self):
        # 1. Load Data
        sales = pl.read_excel(self.sales_file)
        master = pl.read_excel(self.master_file)

        # 2. CREATE YEAR COLUMN (Fixes "Year not found" error)
        # We look for common date column names and extract the Year
        date_col = next((c for c in ["Invoice Date", "Date", "Bill Date"] if c in sales.columns), None)
        
        if date_col:
            # Check if Polars read it as a Date/Datetime object or String
            if sales[date_col].dtype in (pl.Date, pl.Datetime):
                sales = sales.with_columns(pl.col(date_col).dt.year().cast(pl.Int64).alias("Year"))
            else:
                # If it's a string, attempt to convert to date then extract year
                sales = sales.with_columns(
                    pl.col(date_col).str.to_date(strict=False).dt.year().cast(pl.Int64).alias("Year")
                )
        else:
            # Fallback if no date column is found to prevent crash
            sales = sales.with_columns(pl.lit(2024).alias("Year"))

        # 3. Numeric to String Conversion
        # Convert numeric columns to string to prevent join issues (excluding the new Year column)
        num_cols = [c for c, t in sales.schema.items() if t in {pl.Float64, pl.Int64} and c != "Year"]
        if num_cols:
            sales = sales.with_columns(pl.col(num_cols).cast(pl.String))

        # 4. Join and Logic
        cols_to_bring = ['PLI APP', 'PLI CAT', 'CATE ALL', 'PLI HSN', 'UQM']
        
        # Unique Master to prevent row explosion
        master_subset = master.unique(subset=['Product Code']).select(['Product Code'] + cols_to_bring)
        
        df = sales.join(master_subset, on='Product Code', how='left')

        # 5. Apply Pravin Masalewale & Type Logic
        # Use fill_null('') to prevent Regex errors on empty rows
        pravin_mask = pl.col('Customer Name').fill_null('').str.contains('(?i)Pravin Masalewale')
        ic_mask = pl.col('Customer Name').fill_null('').str.contains('(?i)Pravin Sales Division|Aveer Foods Ltd|PRAVIN SALES DIVISION')
        credit_mask = pl.col('Billing Description').fill_null('').str.contains(r'SRN|Credit Rate Diff billing')
        
        self.result_df = df.with_columns(
            pl.when(pravin_mask).then(pl.lit('inter')).otherwise(pl.col('PLI APP')).alias('PLI APP'),
            pl.when(pravin_mask).then(pl.lit('N/A')).otherwise(pl.col('CATE ALL')).alias('CATE ALL'),
            pl.when(ic_mask & credit_mask).then(pl.lit('Credit Note IC'))
            .when(ic_mask).then(pl.lit('Domestic IC'))
            .when(pravin_mask).then(pl.lit('inter'))
            .when(credit_mask).then(pl.lit('Credit Note'))
            .otherwise(pl.lit('Domestic')).alias('Type')
        )
        #priority_cols = ['PLI APP', 'PLI CAT', 'CATE ALL', 'PLI HSN', 'UQM', 'Type', 'Year']
        # Get all other columns that aren't in the priority list
        #other_cols = [c for c in df.columns if c not in priority_cols]
        # Final reordered dataframe
        #self.result_df = df.select(priority_cols + other_cols)

        # 6. Pivot
        # Ensure values are float and fill nulls before pivoting
        self.result_df = self.result_df.with_columns(
            pl.col("Billed Qty(KG)").cast(pl.Float64, strict=False).fill_null(0),
            pl.col("Taxable Value").cast(pl.Float64, strict=False).fill_null(0)
        )

        self.pivot_df = self.result_df.pivot(
            values=["Billed Qty(KG)", "Taxable Value"],
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type", 
            aggregate_function="sum"
        ).fill_null(0)
        
        return self.result_df, self.pivot_df


