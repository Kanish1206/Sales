import polars as pl
import io

class SalesProcessor:
    def __init__(self, sales_file, master_file):
        self.sales_file = sales_file
        self.master_file = master_file

    def _load_data(self, file_obj):
        # Polars native read_excel is more robust than jumping through Pandas
        # We use calamine as it handles xls, xlsx, and xlsb
        return pl.read_excel(io.BytesIO(file_obj.read()), engine="calamine")

    def process(self):
        # 1. Load Data
        sales = self._load_data(self.sales_file)
        master = self._load_data(self.master_file)

        # 2. Rename Columns to avoid conflicts
        sales = sales.rename({
            'PLI APP': 'PLI APP 1',
            'PLI CAT': 'PLI CAT 1',
            'CATE ALL': 'CATE ALL 1',
            'PLI HSN': 'PLI HSN 1',
            'UQM': 'UQM 1'
        })

        # 3. Cast key columns to String for safety
        string_cols = ["Product Code", "Customer Name", "Billing Description"]
        # Ensure columns exist before casting to avoid errors
        actual_string_cols = [c for c in string_cols if c in sales.columns]
        sales = sales.with_columns([
            pl.col(c).cast(pl.String).fill_null("Unknown") for c in actual_string_cols
        ])

        # 4. Join with Master Data
        master_cols = ["Product Code", "PLI APP", "PLI CAT", "CATE ALL", "PLI HSN", "UQM"]
        # Ensure Master has these columns
        master_sub = master.select([c for c in master_cols if c in master.columns])
        
        result_df = sales.join(master_sub, on="Product Code", how="left")

        # 5. Apply Business Logic: Customer-based tagging
        is_pravin = pl.col('Customer Name').str.contains('(?i)Pravin Masalewale')
        
        result_df = result_df.with_columns([
            pl.when(is_pravin).then(pl.lit('inter')).otherwise(pl.col('PLI APP')).alias('PLI APP'),
            pl.when(is_pravin).then(pl.lit('inter')).otherwise(pl.col('PLI CAT')).alias('PLI CAT'),
            pl.when(is_pravin).then(pl.lit('N/A')).otherwise(pl.col('CATE ALL')).alias('CATE ALL'),
            pl.when(is_pravin).then(pl.lit('N/A')).otherwise(pl.col('PLI HSN')).alias('PLI HSN'),
            pl.when(is_pravin).then(pl.lit('N/A')).otherwise(pl.col('UQM')).alias('UQM')
        ])

        # 6. Apply Type and Inter-Company Logic
        ic_names = '(?i)Pravin Sales Division|Aveer Foods Ltd|PRAVIN SALES DIVISION'
        
        result_df = result_df.with_columns(
            pl.when(pl.col('Billing Description').str.contains(r'SRN|Credit Rate Diff billing'))
            .then(pl.lit('Credit Note'))
            .when(pl.col('Billing Description').str.contains(r'Export Direct Billing'))
            .then(pl.lit('Export'))
            .otherwise(pl.lit('Domestic'))
            .alias('Type')
        )

        result_df = result_df.with_columns(
            pl.when((pl.col('Customer Name').str.contains(ic_names)) & (pl.col('Type') == 'Credit Note'))
            .then(pl.lit('Credit Note IC'))
            .when(pl.col('Customer Name').str.contains(ic_names))
            .then(pl.lit('Domestic IC'))
            .when(is_pravin)
            .then(pl.lit('inter'))
            .otherwise(pl.col('Type'))
            .alias('Type')
        )

        # 7. Final Clean-up and Pivoting
        # Cast numeric columns for calculation
        result_df = result_df.with_columns([
            pl.col("Billed Qty(KG)").cast(pl.Float64, strict=False).fill_null(0),
            pl.col("Taxable Value").cast(pl.Float64, strict=False).fill_null(0)
        ])

        # Create Pivot
        pivot_df = result_df.pivot(
            values=["Billed Qty(KG)", "Taxable Value"],
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type",
            aggregate_function="sum"
        ).fill_null(0)

        return result_df, pivot_df
