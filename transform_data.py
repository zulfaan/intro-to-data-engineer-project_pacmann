from clean_amazon import clean_weight, clean_shipping, clean_availability, clean_condition, get_earliest_date
from clean_torch import clean_data_tokped, clean_data_lazada, clean_product_name
from extract_data import ExtractMarketingData, ExtractDatabaseSalesData, ExtractTokpedTorchData, ExtractLazadaTorchData
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
        marketing_data['prices.dateSeen'] = marketing_data['prices.dateSeen'].apply(get_earliest_date)

        marketing_data['weight'] = marketing_data['weight'].apply(clean_weight)

        marketing_data['prices.availability'] = marketing_data['prices.availability'].apply(clean_availability)

        marketing_data['prices.condition'] = marketing_data['prices.condition'].apply(clean_condition)

        marketing_data['prices.shipping'] = marketing_data['prices.shipping'].apply(clean_shipping)

        marketing_data['prices.amountMax'] = pd.to_numeric(marketing_data['prices.amountMax'], errors='coerce')
        marketing_data['prices.amountMin'] = pd.to_numeric(marketing_data['prices.amountMin'], errors='coerce')
        marketing_data['discount'] = round((marketing_data['prices.amountMax'] - marketing_data['prices.amountMin']) / marketing_data['prices.amountMax'] * 100, 0)
        marketing_data['discount'] = marketing_data['discount'].astype(int).astype(str) + '%'
        marketing_data['discount'] = marketing_data['discount'].replace('0%', 'No Discount')

        marketing_data['prices.merchant'] = marketing_data['prices.merchant'].str.replace('.com', '')

        marketing_data['name'] = marketing_data['name'].apply(clean_product_name)
        marketing_data['name'] = marketing_data['name'].str.replace(' " ', ' ')

        columns_to_drop = ['prices.amountMax', 'id', 'prices.currency', 'prices.sourceURLs', 'prices.sourceURLs', 'prices.shipping', 'prices.isSale', 'prices.sourceURLs', 'asins', 'brand', 'categories', 'dateAdded', 'dateUpdated', 'ean', 'imageURLs', 'keys', 'manufacturer', 'manufacturerNumber', 'sourceURLs', 'upc', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30']
        rename_columns = {
            'prices.dateSeen': 'date_seen',
            'prices.availability': 'availability',
            'prices.amountMin': 'price_sale',
            'primaryCategories': 'category',
            'name': 'name_product',
            'weight': 'weight_kg',
            'discount': 'discount',
            'prices.merchant': 'merchant',
            'prices.condition': 'condition'
        }

        marketing_data = marketing_data.drop(columns=columns_to_drop)
        marketing_data = marketing_data.rename(columns=rename_columns)
        marketing_data = marketing_data[['name_product', 'category', 'price_sale', 'discount', 'condition', 'availability', 'date_seen', 'weight_kg', 'merchant']]

        # Save the data
        marketing_data.to_csv("transform-data/marketing_clean.csv", index=False)

class TransformSalesData(luigi.Task):
    def requires(self):
        return ExtractDatabaseSalesData()

    def output(self):
        return luigi.LocalTarget("transform-data/sales_clean.csv")

    def run(self):
        # Load the data
        sales_data = pd.read_csv(self.input().path)
        
        # Clean the data
        sales_data['name'] = sales_data['name'].apply(clean_product_name)
        sales_data['name'] = sales_data['name'].str.replace('"', '')

        sales_data = sales_data.drop_duplicates(subset='name', keep='first')
        sales_data = sales_data.dropna(subset=['actual_price'])

        sales_data['discount_price'] = sales_data['discount_price'].fillna(sales_data['actual_price'])

        sales_data['discount_price'] = sales_data['discount_price'].str.replace('₹', '').str.replace(',', '').astype(float)
        sales_data['actual_price'] = sales_data['actual_price'].str.replace('₹', '').str.replace(',', '').astype(float)

        sales_data['discount'] = round((sales_data['actual_price'] - sales_data['discount_price']) / sales_data['actual_price'] * 100, 0)
        sales_data['discount'] = sales_data['discount'].astype(int).astype(str) + '%'
        sales_data['discount'] = sales_data['discount'].replace('0%', 'No Discount')

        sales_data['ratings'] = sales_data['ratings'].fillna('No Ratings')
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].fillna('No Ratings')

        columns_to_drop = ['sub_category', 'image', 'actual_price', 'Unnamed: 0']
        sales_data = sales_data.drop(columns=columns_to_drop)

        category_mapping = {
            "women's clothing": "clothing",
            "men's clothing": "clothing",
            "kids' fashion": "kids and baby products",
            "toys & baby products": "kids and baby products",
            "home & kitchen": "home and kitchen",
            "home, kitchen, pets": "home and kitchen",
            "women's shoes": "footwear",
            "men's shoes": "footwear",
            "beauty & health": "beauty and health",
            "grocery & gourmet foods": "grocery",
            "tv, audio & cameras": "electronics",
            "car & motorbike": "automotive",
        }
        sales_data['main_category'] = sales_data['main_category'].replace(category_mapping)

        rename_columns = {
            'name': 'name_product',
            'main_category': 'category',
            'discount_price': 'price_sale_rupee',
            'discount': 'discount',
            'ratings': 'rating',
            'no_of_ratings': 'rating_count'
        }
        sales_data = sales_data.rename(columns=rename_columns)
        
        sales_data['price_sale_rupee'] = sales_data['price_sale_rupee'].astype(int)

        # Save the data
        sales_data.to_csv("transform-data/sales_clean.csv", index=False)

if __name__ == '__main__':
    luigi.build([TransformTorchData(), TransformMarketingData(), TransformSalesData()], local_scheduler=True)