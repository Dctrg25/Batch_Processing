import pandas as pd
import os
from datetime import datetime
from dags.ETL_S3_Postgres.Transform.Transform import Transform_df

class Transform_sale_df(Transform_df):
    def extract_var(self):
        filePath = os.path.join(self.root_dir, "Products.csv")
        product_df = pd.read_csv(filePath)

        product_df = [(product_df['PRODUCT_ID'][i], 
                      product_df['COMMISION RATE'][i]) for i in range(len(product_df))]

        self.product_df = sorted(product_df, key= lambda x : x[0])
        self.n = len(self.df)

        self.product = list(self.df['Product ID'])
        self.total_cost = list(self.df['Total cost'])

    def search_product(self, sale_product_id): # binary search
        l = 0
        r = len(self.product_df) - 1

        while(l <= r) :
            mid = int((l + r) / 2)
            product_id, product_comm_rate = self.product_df[mid]

            if (product_id > sale_product_id) : 
                r = mid - 1
            elif (product_id < sale_product_id): 
                l = mid + 1
            else:
                return product_id, product_comm_rate
            
    def transform(self):
        self.df['Date'] = [datetime.strptime(date, "%m-%d-%y").date() 
                            for date in self.df['Date']]
        
        # Create `revenue_arr` list which stores 'product_comm_rate' of each 'sale_product_id'
        revenue_arr = [self.search_product(self.product[i]) for i in range(self.n)]

        self.df['Profit'] = [(self.total_cost[i] * val[1] /100) for i, val in enumerate(revenue_arr)]

def Transform_sales(Name, filePath):
    sale = Transform_sale_df(Name, filePath)