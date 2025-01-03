import pandas as pd
import re

def get_earliest_date(dates):
    date_list = [pd.to_datetime(date.strip(), format="%Y-%m-%dT%H:%M:%SZ") for date in dates.split(",")]
    earliest_date = min(date_list)
    
    return earliest_date.strftime("%Y-%m-%d")

def clean_weight(weight):
    weight = weight.strip().lower()
    weight_parts = re.findall(r'(\d+\.?\d*)\s*(lbs?|pounds?|ounces?|oz)', weight)
    total_weight = 0.0

    for value, unit in weight_parts:
        value = float(value) 
        if unit in ['lbs', 'lb', 'pounds']:
            total_weight += value * 0.453592 
        elif unit in ['ounces', 'ounce', 'oz']:
            total_weight += value * 0.0283495 
    
    return round(total_weight, 2)

def clean_shipping(value):
    if pd.isna(value):
        return 'Undefined'
    value = str(value).lower()
    if 'free' in value:
        return 'Free Shipping'
    elif 'expedited' in value:
        return 'Expedited Shipping'
    elif 'standard' in value:
        return 'Standard Shipping'
    elif 'freight' in value:
        return 'Freight Shipping'
    elif 'usd' in value or 'cad' in value:
        return 'Paid Shipping'
    else:
        return 'Undefined'

def clean_availability(value):
    if pd.isna(value):
        return 'Unavailable'
    value = str(value).lower()
    if 'in stock' or 'yes' or 'true' or ' availaible' in value:
        return 'Available'
    elif 'out of stock' or 'no' or 'retired' or 'sold' or 'false' in value:
        return 'Unavailable'
    elif 'special order' or 'more on the way' in value:
        return 'Pending'
    else:
        return 'Unavailable'

def clean_condition(condition):
    if pd.isna(condition):
        return 'Undefined'
    condition = str(condition).lower()
    if 'new' in condition:
        return 'New'
    else:
        return 'Used'