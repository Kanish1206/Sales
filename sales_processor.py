import polars as pl
import io

class SalesProcessor:
    def __init__(self, sales_file, master_file):
        # We read the uploaded files from Streamlit (BytesIO)
        self.sales = pl.read_excel(sales_file.read())
        self.master = pl.read_excel(master_file.read())

    def process(self):
        # 1. Rename Columns to avoid conflicts during join
        sales = self.sales.rename({
            'PLI APP': 'PLI APP 1',
            'PLI CAT': 'PLI CAT 1',
            'CATE ALL': 'CATE ALL 1',
            'PLI HSN': 'PLI HSN 1',
            'UQM': 'UQM 1'
        })

        # 2. Cast numeric columns to String for safety (as per your original code)
        Num_data = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, 
                    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, 
                    pl.Float32, pl.Float64}
        
        num_cols = [name for name, dtype in sales.schema.items() if dtype in Num_data]
        sales = sales.with_columns(pl.col(num_cols).cast(pl.String))

        # 3. Join with Master Data
        master_cols = ["Product Code", "PLI APP", "PLI CAT", "CATE ALL", "PLI HSN", "UQM"]
        result_df = sales.join(self.master.select(master_cols), on="Product Code", how="left")

        # 4. Apply Business Logic: Customer-based tagging
        result_df = result_df.with_columns([
            pl.when(pl.col('Customer Name').str.contains('(?i)Pravin Masalewale'))
            .then(pl.lit('inter')).otherwise(pl.col('PLI APP')).alias('PLI APP'),
            
            pl.when(pl.col('Customer Name').str.contains('(?i)Pravin Masalewale'))
            .then(pl.lit('inter')).otherwise(pl.col('PLI CAT')).alias('PLI CAT'),
            
            pl.when(pl.col('Customer Name').str.contains('(?i)Pravin Masalewale'))
            .then(pl.lit('N/A')).otherwise(pl.col('CATE ALL')).alias('CATE ALL'),
            
            pl.when(pl.col('Customer Name').str.contains('(?i)Pravin Masalewale'))
            .then(pl.lit('N/A')).otherwise(pl.col('PLI HSN')).alias('PLI HSN'),
            
            pl.when(pl.col('Customer Name').str.contains('(?i)Pravin Masalewale'))
            .then(pl.lit('N/A')).otherwise(pl.col('UQM')).alias('UQM')
        ])

        # 5. Apply Type Logic (Domestic, Export, Credit Note)
        result_df = result_df.with_columns(
            pl.when(pl.col('Billing Description').str.contains(r'SRN|Credit Rate Diff billing'))
            .then(pl.lit('Credit Note'))
            .when(pl.col('Billing Description').str.contains(r'Export  Direct Billing'))
            .then(pl.lit('Export'))
            .otherwise(pl.lit('Domestic'))
            .alias('Type')
        )

        # 6. Apply Inter-Company (IC) Logic
        ic_names = '(?i)Pravin Sales Division|Aveer Foods Ltd|PRAVIN SALES DIVISION'
        result_df = result_df.with_columns(
            pl.when((pl.col('Customer Name').str.contains(ic_names)) & 
                    (pl.col('Billing Description').str.contains('SRN|Credit Rate Diff billing')))
            .then(pl.lit('Credit Note IC'))
            .when(pl.col('Customer Name').str.contains(ic_names))
            .then(pl.lit('Domestic IC'))
            .when(pl.col('Customer Name').str.contains('(?i)Pravin Masalewale'))
            .then(pl.lit('inter'))
            .otherwise(pl.col('Type'))
            .alias('Type')
        )

        # 7. Final Clean-up and Pivoting
        # Cast back to Float for math
        result_df = result_df.with_columns([
            pl.col("Billed Qty(KG)").cast(pl.Float64, strict=False),
            pl.col("Taxable Value").cast(pl.Float64, strict=False)
        ])

        pivot_df = result_df.pivot(
            values=["Billed Qty(KG)", "Taxable Value"],
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type",
            aggregate_function="sum"
        )

        return result_df, pivot_df
