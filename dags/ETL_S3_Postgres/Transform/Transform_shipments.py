from dags.ETL_S3_Postgres.Transform.Transform import Transform_df

class Transform_shipment_df(Transform_df):
    def transform(self):
        self.df.drop_duplicates(subset=['Order ID'], keep='first', inplace= True)

        self.df['Shipping status'] = self.df['Shipping status'].str.lower()

        self.df['Shipping address'] = [address.split(',')[0] for address in self.df['Destination']]
        self.df['Shipping zipcode'] = [address.split(',')[4] for address in self.df['Destination']]

        self.df['Shipping address zipcode'] = self.df['Shipping address'].astype(str) + '-' + self.df['Shipping zipcode'].astype(str)

        self.df.drop(columns=['Destination'], inplace=True)

def Transform_shipments(Name, filePath):
    shipment = Transform_shipment_df(Name, filePath)        