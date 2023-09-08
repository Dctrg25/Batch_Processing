from dags.ETL_S3_Postgres.Transform.Transform import Transform_df

class Transform_product_df(Transform_df):
    def transform(self):
        # PRODUCT_SIZE column is an unecessary column
        self.df['PRODUCT_SIZE'] = [0 for i in range(len(self.df))]
        # fillna missing values
        self.df['BRAND'].fillna('Unknown', inplace=True)

        # create new column from sell price and commision rate column
        self.df['COMMISION'] = self.df['SELL PRICE'] * self.df['COMMISION RATE'] / 100

def Transform_products(Name, filePath):
    product = Transform_product_df(Name, filePath)