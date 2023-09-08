import pandas as pd
import os 
from dags.ETL_S3_Postgres.Transform.Rename_column import column_dict

class Transform_df : # Parent class for transformation of dataframe
    def __init__(self, Name, filepath = ""):
        self.root_dir = "/home/truong/airflow/dags/project_batch/data"
        self.write_dir = "/home/truong/airflow/dags/project_batch/Transformed_data"

        try:
            path = os.path.join(self.root_dir, filepath)
            self.df = pd.read_csv(path)

        except:
            self.df = pd.DataFrame()

        self.name = Name

        self.clean()
        self.extract_var()
        self.transform()
        self.rename_column()
        self.write_csv()

    def extract_var(self):
        pass

    def get_primary_key(self):
        for col in self.df.columns:
            if ("ID" in col):
                return col
            
    def clean(self):
        self.df.drop_duplicates(subset= [self.get_primary_key()], keep= 'first', inplace= True)

    def transform(self):
        pass

    def rename_column(self):
        self.df.rename(columns= column_dict[self.name], inplace= True)

    def write_csv(self):
        write_path = os.path.join(self.write_dir, self.name + ".csv")
        self.df.to_csv(write_path, index=False)