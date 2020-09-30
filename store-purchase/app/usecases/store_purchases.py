# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import StorePurchaseQuery
from utils.read_params import ReadParams


class StorePurchases(StorePurchaseQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def blocket_data_store_purchases(self):
        return self.__blocket_data_store_purchases

    @blocket_data_store_purchases.setter
    def blocket_data_store_purchases(self, config):
        db_source = Database(conf=config)
        blocket_data_store_purchases = db_source.select_to_dict(self.blocket_store_purchases())
        db_source.close_connection()
        self.__blocket_data_store_purchases = blocket_data_store_purchases

    def insert_to_table(self):
        dwh = Database(conf=self.config.db)   
        dwh.execute_command(self.clean_table())
        dwh.insert_copy(self.cleaned_data, "ods", "store_purchase")

    def generate(self):
        self.blocket_data_store_purchases = self.config.blocket
        self.cleaned_data = self.blocket_data_store_purchases
        self.cleaned_data["product_id_fk"] = 0
        for column in ["store_id_nk",
                       "user_id",
                       "region",
                       "total_price",
                       "product_id_fk"]:
            self.cleaned_data[column] = self.cleaned_data[column].astype('Int64')

        self.logger.info("First records as evidence")
        self.logger.info(self.cleaned_data.head())
        self.insert_to_table()
        self.logger.info("Store purchases succesfully saved")
        return True

            


