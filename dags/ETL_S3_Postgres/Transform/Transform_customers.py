from dags.ETL_S3_Postgres.Transform.Transform import Transform_df

class Transform_customer_df(Transform_df):
    def transform(self):
        self.df.drop(columns= ['City', 'State', 'Country'], inplace= True)
        self.df['Address_Postal_code'] = self.df['Address'] + '-' + self.df['Postal code'].astype(str)

def Transform_customers(Name, filePath):
    customer = Transform_customer_df(Name, filePath)