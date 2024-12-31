from extract_data import ExtractMarketingData, ExtractDatabaseSalesData, ExtractTokpedTorchData, ExtractLazadaTorchData
from clean_func import clean_data_tokped, clean_data_lazada
import pandas as pd
import luigi


class TransformTorchData(luigi.Task):
    def requires(self):
        return [ExtractTokpedTorchData(), ExtractLazadaTorchData()]

    def output(self):
        return luigi.LocalTarget("transform_data/torch_tokped_clean.csv",
        "transform-data/torch_lazada_clean.csv")

    def run(self):
        # Load the data
        tokped_data = pd.read_csv(self.input()[0].path)
        lazada_data = pd.read_csv(self.input()[1].path)

        # Clean the data
        tokped_data = clean_data_tokped(tokped_data)
        lazada_data = clean_data_lazada(lazada_data)
        
        # Save the data
        tokped_data.to_csv("transform-data/torch_tokped_clean.csv", index=False)
        lazada_data.to_csv("transform-data/torch_lazada_clean.csv", index=False)

class TransformMarketingData(luigi.Task):
    def requires(self):
        return ExtractMarketingData()

    def output(self):
        return luigi.LocalTarget("transform-data/marketing_clean.csv")

    def run(self):
        # Load the data
        marketing_data = pd.read_csv(self.input().path)
        
        # Clean the data
        marketing_data['date'] = pd.to_datetime(marketing_data['date'])
        
        # Save the data
        marketing_data.to_csv("transform-data/marketing_clean.csv", index=False)

if __name__ == '__main__':
    luigi.build([TransformTorchData()], local_scheduler=True)