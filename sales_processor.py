import polars as pl

class SalesProcessor:
    def __init__(self, sales_file, master_file):
        self.sales_file = sales_file
        self.master_file = master_file

    def process(self):
        # Explicitly use calamine engine as seen in your logs
        # infer_schema_length=0 treats everything as string initially to prevent crashes
        sales = pl.read_excel(self.sales_file, engine="calamine", infer_schema_length=1000)
        master = pl.read_excel(self.master_file, engine="calamine", infer_schema_length=1000)

        # Clean column names (remove leading/trailing spaces)
        sales = sales.rename({c: c.strip() for c in sales.columns})
        master = master.rename({c: c.strip() for c in master.columns})

        # Rename existing columns only
        rename_map = {
            'PLI APP': 'PLI APP 1', 'PLI CAT': 'PLI CAT 1',
            'CATE ALL': 'CATE ALL 1', 'PLI HSN': 'PLI HSN 1', 'UQM': 'UQM 1'
        }
        sales = sales.rename({k: v for k, v in rename_map.items() if k in sales.columns})

        # Join
        cols_to_bring = ['PLI APP', 'PLI CAT', 'CATE ALL', 'PLI HSN', 'UQM']
        # Only bring columns that exist in master
        cols_to_bring = [c for c in cols_to_bring if c in master.columns]
        
        df = sales.join(master.select(['Product Code'] + cols_to_bring), on='Product Code', how='left')

        # Logic
        # Using (?i) for case-insensitive matching
        pravin_mask = pl.col('Customer Name').str.contains('(?i)Pravin Masalewale')
        ic_mask = pl.col('Customer Name').str.contains('(?i)Pravin Sales Division|Aveer Foods Ltd|PRAVIN SALES DIVISION')
        credit_mask = pl.col('Billing Description').str.contains(r'SRN|Credit Rate Diff billing')

        res = df.with_columns(
            pl.when(pravin_mask).then(pl.lit('inter')).otherwise(pl.col('PLI APP')).alias('PLI APP'),
            pl.when(pravin_mask).then(pl.lit('N/A')).otherwise(pl.col('CATE ALL')).alias('CATE ALL'),
            pl.when(ic_mask & credit_mask).then(pl.lit('Credit Note IC'))
            .when(ic_mask).then(pl.lit('Domestic IC'))
            .when(pravin_mask).then(pl.lit('inter'))
            .when(credit_mask).then(pl.lit('Credit Note'))
            .otherwise(pl.lit('Domestic')).alias('Type')
        )

        # IMPORTANT: Force numeric types before pivoting
        # This fixes the "fallback to string" issue in your logs
        val_cols = ["Billed Qty(KG)", "Taxable Value"]
        for col in val_cols:
            if col in res.columns:
                res = res.with_columns(pl.col(col).cast(pl.Float64, strict=False).fill_null(0))

        pivot = res.pivot(
            values=val_cols,
            index=["Year", "PLI APP", "PLI CAT", "PLI HSN", "UQM"],
            columns="Type", 
            aggregate_function="sum"
        ).fill_null(0)
        
        return res, pivot
