import csv, os
import pandas as pd

from stl.persistence import PersistenceInterface

class Csv(PersistenceInterface):

    def __init__(self, csv_path: str):
        self.__csv_path = csv_path

    def save(self, query: str, listings: list):
        file_exists = os.path.isfile(self.__csv_path)
        with open(self.__csv_path, 'a', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=listings[0].keys())
            if not file_exists:
                writer.writeheader()
            writer.writerows(listings)
            
            # Drop duplicates
            df = pd.read_csv(self.__csv_path)
            df.drop_duplicates(subset='id', keep='last', inplace=True)
            df.to_csv(self.__csv_path, index=False)
