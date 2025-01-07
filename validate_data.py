from etl_de_project_pacmann import ExtractLazadaTorchData, ExtractTokpedTorchData, ExtractDatabaseSalesData, ExtractMarketingData
from tabulate import tabulate
import pandas as pd
import luigi
import os

class ValidateData(luigi.Task):
    def requires(self):
        return [ExtractMarketingData(), ExtractDatabaseSalesData(), ExtractTokpedTorchData(), ExtractLazadaTorchData()]

    def run(self):
        # Buat direktori jika belum ada
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        
        raw_marketing_data = pd.read_csv(self.input()[0].path)
        raw_sales_data = pd.read_csv(self.input()[1].path)
        raw_tokped_data = pd.read_csv(self.input()[2].path)
        raw_laza_data = pd.read_csv(self.input()[3].path)

        list_df = {
            'raw_marketing_data': raw_marketing_data,
            'raw_sales_data': raw_sales_data,
            'raw_laza_data': raw_laza_data,
            'raw_tokped_data': raw_tokped_data
        }
        
        with open(self.output().path, 'w', newline='') as f:
            # Check Data Shape
            f.write("========== Check Data Shape ==========\n\n")
            result_df = []
            for name, df in list_df.items():
                n_columns = df.shape[1]
                n_rows = df.shape[0]
                result_df.append([name, n_columns, n_rows])
                
            headers_shape = ['Dataframe', 'Columns', 'Rows']
            f.write(tabulate(result_df, headers_shape, tablefmt="grid"))
            f.write("\n\n")

            # Check Data Values
            for name, df in list_df.items():
                result_val = []
                for col in df.columns:
                    col_type = df[col].dtype
                    sum_na = round(df[col].isna().sum() * 100 / len(df))
                    sum_dup = round(df.duplicated(keep=False).sum())
                    result_val.append([col, col_type, sum_na, sum_dup])

                headers_val = ['Column Name', 'Data Type', 'Missing Values (%)', 'Duplicate Values (count)']
                f.write(f"Checking Data Values: {name}\n")
                f.write(tabulate(result_val, headers_val, tablefmt="grid"))
                f.write("\n\n")

    def output(self):
        return luigi.LocalTarget('validate-data/validate_data.txt')

if __name__ == "__main__":
    luigi.build([ValidateData()], local_scheduler=True)
    print("Task complete.")