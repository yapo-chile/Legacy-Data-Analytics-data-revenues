# pylint: disable=no-member
# utf-8
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class ProductOrder():
    def __init__(self,
                 config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params

    @property
    def data_product_order_blocket(self):
        return self.__data_dwh

    @data_product_order_blocket.setter
    def data_product_order_blocket(self, config):
        blocket = Database(conf=config)
        data_dwh = blocket.select_to_dict(
            Query(config, self.params).query_get_product_order_blocket())
        blocket.close_connection()
        self.__data_product_order_blocket= data_dwh

    # Write data to data warehouse
    def save_product_order_stg(self) -> None:
        db = Database(conf=self.config.db)
        db.execute_command(
            Query(self.config, self.params).query_truncate_product_order_stg())
        db.insert_copy(self.data_product_order_blocket)
        db.close_connection()

    @property
    def data_product_order_stg(self):
        return self.__data_dwh

    @data_product_order_stg.setter
    def data_product_order_stg(self, config):
        dw = Database(conf=config)
        data_dwh = dw.select_to_dict(
            Query(config, self.params).query_get_product_order_stg())
        dw.close_connection()
        self.__data_product_order_stg = data_dwh

    def save_product_order_ods(self) -> None:
        db = Database(conf=self.config.db)
        db.execute_command(
            Query(self.config, self.params).query_delete_product_order_ods())
        db.insert_copy(self.data_product_order_stg)
        db.close_connection()

    def generate(self):
        self.data_product_order_blocket = self.config.blocket
        self.save_product_order_stg()
        self.data_product_order_stg = self.config.db
        self.save_product_order_ods()