# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import Query


class BlocketPuchases:
    def __init__(self, config, params) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('BlocketPuchases')

    def save(self, data, schema, table_name, configdb) -> None:
        db = Database(conf=configdb)
        self.logger.info('Iniciando inserción de datos')
        db.insert_copy(schema, table_name, data)
        self.logger.info(
            'Datos insertados en {}.{}'.format(schema, table_name))
        db.close_connection()

    def truncate_stg_purchase_ios(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Truncando stg.purchase_ios')
        db.execute_command(query.tr_stg_purchase_ios())
        db.close_connection()

    @property
    def stg_purchase_ios(self):
        return self.__stg_purchase_ios

    @stg_purchase_ios.setter
    def stg_purchase_ios(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.stg_purchase_ios())
        db.close_connection()
        self.__stg_purchase_ios = data

    def truncate_product_order_detail(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Truncando stg.product_order_detail')
        db.execute_command(query.tr_product_order_detail())
        db.close_connection()

    @property
    def product_order_detail(self):
        return self.__product_order_detail

    @product_order_detail.setter
    def product_order_detail(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.product_order_detail())
        db.close_connection()
        data = data.astype(
            {
                'purchase_detail_id_nk': 'Int64',
                'num_days': 'Int64',
                'total_bump': 'Int64',
                'frequency': 'Int64'
            }
        )
        self.__product_order_detail = data

    def generate(self):
        self.stg_purchase_ios = self.config.db
        self.truncate_stg_purchase_ios()
        self.save(self.stg_purchase_ios,
                  'stg',
                  'purchase_ios',
                  self.config.dw)
        self.product_order_detail = self.config.db
        self.truncate_product_order_detail()
        self.save(self.product_order_detail,
                  'stg',
                  'product_order_detail',
                  self.config.dw)
