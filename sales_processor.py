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

        # 2. Rename Columns
        sales = sales.rename({
            'PLI APP': 'PLI APP 1', 'PLI CAT': 'PLI CAT 1',
            'CATE ALL': 'CATE ALL 1', 'PLI HSN': 'PLI HSN 1', 'UQM': 'UQM 1'
        })

        # 3. Numeric to String Conversion
        num_cols = [c for c, t in sales.schema.items() if t in {pl.Float64, pl.Int64}]
        sales = sales.with_columns(pl.col(num_cols).cast(pl.String))

        # 4. Join and Logic
        cols_to_bring = ['PLI APP', 'PLI CAT', 'CATE ALL', 'PLI HSN', 'UQM']
        df = sales.join(master.select(['Product Code'] + cols_to_bring), on='Product Code', how='left')

        # 5. Apply Pravin Masalewale & Type Logic
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

        # 6. Pivot
        self.result_df = self.result_df.with_columns(pl.col(["Billed Qty(KG)", "Taxable Value"]).cast(pl.Float64))
        self.pivot_df = self.result_df.pivot(
            values=["Billed Qty(KG)", "Taxable Value"],
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type", aggregate_function="sum"
        ).fill_null(0)
        
        return self.result_df, self.pivot_df