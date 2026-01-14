import polars as pl
import io

class SalesProcessor:
    def __init__(self, sales_file, master_file):
        # We wrap in BytesIO to ensure Polars can seek through the file buffer
        # Using 'calamine' engine for better compatibility with .xlsb and .xlsx
        self.sales = pl.read_excel(io.BytesIO(sales_file.read()), engine="calamine")
        self.master = pl.read_excel(io.BytesIO(master_file.read()), engine="calamine")

    def process(self):
        # 1. Rename Columns to avoid conflicts during join
        sales = self.sales.rename({
            'PLI APP': 'PLI APP 1',
            'PLI CAT': 'PLI CAT 1',
            'CATE ALL': 'CATE ALL 1',
            'PLI HSN': 'PLI HSN 1',
            'UQM': 'UQM 1'
        })

        # 2. Cast key columns to String for safe joining/regex
        # Instead of all numbers, we target columns usually used for logic
        string_cols = ["Product Code", "Customer Name", "Billing Description"]
        sales = sales.with_columns([
            pl.col(c).cast(pl.String).fill_null("Unknown") for c in string_cols if c in sales.columns
        ])

        # 3. Join with Master Data
        master_cols = ["Product Code", "PLI APP", "PLI CAT", "CATE ALL", "PLI HSN", "UQM"]
        result_df = sales.join(
            self.master.select(master_cols), 
            on="Product Code", 
            how="left"
        )

        # 4. Apply Business Logic: Customer-based tagging
        # Using a mask for cleaner logic
        is_pravin_masalewale = pl.col('Customer Name').str.contains('(?i)Pravin Masalewale')
        
        result_df = result_df.with_columns([
            pl.when(is_pravin_masalewale).then(pl.lit('inter')).otherwise(pl.col('PLI APP')).alias('PLI APP'),
            pl.when(is_pravin_masalewale).then(pl.lit('inter')).otherwise(pl.col('PLI CAT')).alias('PLI CAT'),
            pl.when(is_pravin_masalewale).then(pl.lit('N/A')).otherwise(pl.col('CATE ALL')).alias('CATE ALL'),
            pl.when(is_pravin_masalewale).then(pl.lit('N/A')).otherwise(pl.col('PLI HSN')).alias('PLI HSN'),
            pl.when(is_pravin_masalewale).then(pl.lit('N/A')).otherwise(pl.col('UQM')).alias('UQM')
        ])

        # 5 & 6. Apply Type and Inter-Company Logic
        ic_names = '(?i)Pravin Sales Division|Aveer Foods Ltd|PRAVIN SALES DIVISION'
        
        result_df = result_df.with_columns(
            pl.when(pl.col('Billing Description').str.contains(r'SRN|Credit Rate Diff billing'))
            .then(pl.lit('Credit Note'))
            .when(pl.col('Billing Description').str.contains(r'Export Direct Billing'))
            .then(pl.lit('Export'))
            .otherwise(pl.lit('Domestic'))
            .alias('Type')
        ).with_columns(
            pl.when((pl.col('Customer Name').str.contains(ic_names)) & 
                    (pl.col('Type') == 'Credit Note'))
            .then(pl.lit('Credit Note IC'))
            .when(pl.col('Customer Name').str.contains(ic_names))
            .then(pl.lit('Domestic IC'))
            .when(is_pravin_masalewale)
            .then(pl.lit('inter'))
            .otherwise(pl.col('Type'))
            .alias('Type')
        )

        # 7. Final Clean-up and Pivoting
        result_df = result_df.with_columns([
            pl.col("Billed Qty(KG)").cast(pl.Float64, strict=False).fill_null(0),
            pl.col("Taxable Value").cast(pl.Float64, strict=False).fill_null(0)
        ])

        # Ensure index columns don't have nulls before pivoting
        pivot_df = result_df.pivot(
            values=["Billed Qty(KG)", "Taxable Value"],
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type",
            aggregate_function="sum"
        ).fill_null(0)

        return result_df, pivot_df
