import os
import pandas as pd
from placekey.api import PlacekeyAPI
import json

# Configuration
PLACEKEY_API_KEY = os.environ.get('PLACEKEY_API_KEY')  # Store API key in environment variable
DATA_DIR = 'data'

# Helper functions
def get_df_for_api(df, column_map):
    """Prepares a DataFrame for the Placekey API.

    Args:
        df (pd.DataFrame): Input DataFrame.
        column_map (dict): Mapping of original column names to Placekey API field names.

    Returns:
        pd.DataFrame: DataFrame ready for Placekey API input.
    """

    df = df.rename(columns=column_map)
    df = df[list(column_map.values())]
    df['iso_country_code'] = 'US'
    df['query_id'] = df['query_id'].astype(str)
    if 'postal_code' in df.columns:
        df['postal_code'] = df['postal_code'].astype(str)
    if 'region' in df.columns:
        df['region'] = df['region'].astype(str)
    return df

def process_dataset(df, column_map, output_filename):
    """Processes a dataset through the Placekey API.

    Args:
        df (pd.DataFrame): Input DataFrame.
        column_map (dict): Mapping of column names.
        output_filename (str): Name of the CSV file to store Placekeys.

    Returns:
        pd.DataFrame: DataFrame with Placekeys.
    """

    # json.loads(Blight_Violations_for_api.to_json(orient="records"))
    df_for_api = get_df_for_api(df, column_map)
    data_json = json.loads(df_for_api.to_json(orient="records"))
    responses = pk_api.lookup_placekeys(data_json, verbose=True)
    placekeys_df = pd.read_json(json.dumps(responses), dtype={'query_id':str})
    placekeys_df.to_csv(os.path.join(DATA_DIR, output_filename), index=False)
    return placekeys_df

if __name__ == '__main__':
    if not PLACEKEY_API_KEY:
        raise ValueError("Please set the PLACEKEY_API_KEY environment variable.")

    pk_api = PlacekeyAPI(PLACEKEY_API_KEY)

    # Process Blight Violations
    blight_df = pd.read_csv(os.path.join(DATA_DIR, 'Blight_Violations.csv'))
    blight_df['zip_code'].astype(str)
    blight_column_map = {
        "ticket_id": "query_id",
        "violation_address" : "street_address",
        "state": "region",
        "zip_code": "postal_code",
        "X" : "latitude",
        "Y" : "longitude",
        "country" : "iso_country_code",
        "city" : "city"
        }
    placekeys_Blight_Violations = process_dataset(blight_df, blight_column_map, 'placekeys_Blight_Violations.csv')
    placekeys_Blight_Violations['ticket_id'] = placekeys_Blight_Violations['query_id'].astype(str)
    blight_df['ticket_id'] = blight_df['ticket_id'].astype(str)
    Blight_Violations_w_placekeys = blight_df.merge(placekeys_Blight_Violations, on='ticket_id', how='left')
    Blight_Violations_w_placekeys.to_csv(os.path.join(DATA_DIR, 'Blight_Violations_w_placekeys.csv'), index=False)

    # Process Property Sales
    sales_df = pd.read_csv(os.path.join(DATA_DIR, 'Property_Sales.csv'))
    Property_Sales_column_map = {
        "sale_id": "query_id",
        "address" : "street_address",
        "X" : "latitude",
        "Y" : "longitude"
        }
    placekeys_Property_Sales = process_dataset(sales_df, Property_Sales_column_map, 'placekeys_Property_Sales.csv') 
    placekeys_Property_Sales['sale_id'] = placekeys_Property_Sales['query_id']
    sales_df['sale_id'] = sales_df['sale_id'].astype(str)
    Property_Sales_w_placekeys = sales_df.merge(placekeys_Property_Sales, on='sale_id', how='left')
    Property_Sales_w_placekeys.to_csv(os.path.join(DATA_DIR, 'Property_Sales_w_placekeys.csv'), index=False)