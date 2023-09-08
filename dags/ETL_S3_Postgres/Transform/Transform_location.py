import pandas as pd
import os 
from dags.ETL_S3_Postgres.Transform.Transform import Transform_df

class Transform_location_df(Transform_df):
    def extract_var(self):
        filePath = os.path.join(self.root_dir, "Customers.csv")
        customer_df = pd.read_csv(filePath)

        filePath = os.path.join(self.root_dir, "Shipments.csv")
        shipment_df = pd.read_csv(filePath)

        self.customer_df = customer_df[['Address','Postal code', 'City', 'State', 'Country']]
        self.shipment_df = shipment_df['Destination']

    def transform(self):
        self.customer_df['Address_Postal_code'] = self.customer_df['Address'].astype('str') + '-' + self.customer_df['Postal code'].astype('str')
        self.customer_df.drop(columns=['Address', 'Postal code'], inplace=True)

        address_arr = [address.split(',') for address in self.shipment_df]

        location_dict = {}
        location_dict['Address'] = [address[0] for address in address_arr]
        location_dict['Postal code'] = [address[4] for address in address_arr]
        location_dict['City']        = [address[1] for address in address_arr]
        location_dict['State']       = [address[2] for address in address_arr]
        location_dict['Country']     = [address[3] for address in address_arr]       

        location_df = pd.DataFrame(location_dict)
        location_df['Address_Postal_code'] = location_df['Address'].astype('str') + '-' + location_df['Postal code'].astype('str')
        location_df.drop(columns=['Address', 'Postal code'], inplace=True)

        # concat customer_df and location_df
        self.df = pd.concat([self.customer_df, location_df])
        self.df = self.df[['Address_Postal_code', 'City', 'State', 'Country']]
        self.df.drop_duplicates(subset=['Address_Postal_code'], inplace=True)

def Transform_locations(Name, filePath):
    location = Transform_location_df(Name, filePath)