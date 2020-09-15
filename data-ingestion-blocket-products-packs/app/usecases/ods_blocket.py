# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from infraestructure.conf import getConf
from utils.query import Query
from utils.read_params import ReadParams


class OdsBlocket:
    def __init__(self, conf: getConf,
                 params: ReadParams) -> None:
        self.config = conf
        self.params = params
        self.logger = logging.getLogger('BlocketPacks')
        self.select_purchase_ios = [
            'product_order_nk',
            'creation_date',
            'payment_date',
            'price',
            'status',
            'insert_date',
            'product_id_nk',
            'ad_id_nk',
            'user_id_nk',
            'payment_platform',
            'price_clp'
        ]

    def save(self, data, schema, table_name, configdb) -> None:
        db = Database(conf=configdb)
        self.logger.info('Iniciando inserción de datos')
        db.insert_copy(schema, table_name, data)
        self.logger.info(
            'Datos insertados en {}.{}'.format(schema, table_name))
        db.close_connection()

    def delete_packs(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Iniciando query: Delete de Packs')
        db.execute_command(query.delete_ods_packs())
        db.close_connection()

    @property
    def stg_packs(self):
        return self.__stg_packs

    @stg_packs.setter
    def stg_packs(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.stg_packs())
        data = data.astype(
            {
                'account_id': 'Int64',
                'days': 'Int64',
                'slots': 'Int64',
                'product_id': 'Int64',
                'seller_id_fk': 'Int64'
            }
        )
        self.__stg_packs = data
        db.close_connection()

    def delete_ods_product_order_ios(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        db.execute_command(
            query.del_ods_product_order_ios())
        db.close_connection()

    @property
    def dw_ods_product_order_ios(self):
        return self.__dw_ods_product_order_ios

    @dw_ods_product_order_ios.setter
    def dw_ods_product_order_ios(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.dw_ods_product_order_ios())
        data = data.astype(
            {
                'price_clp': 'Int64'
            }
        )
        self.__dw_ods_product_order_ios = data
        db.close_connection()

    def delete_ods_product_order_detail(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        db.execute_command(
            query.del_ods_product_order_detail())
        db.close_connection()

    @property
    def ods_product_order_detail(self):
        return self.__ods_product_order_detail

    @ods_product_order_detail.setter
    def ods_product_order_detail(self, config):
        query = Query(config, self.params)
        db = Database(conf=config)
        data = db.select_to_dict(
            query.ods_product_order_detail())
        data = data.astype(
            {
                'purchase_detail_id_nk': 'Int64',
                'num_days': 'Int64',
                'total_bump': 'Int64',
                'frequency': 'Int64'
            }
        )
        db.close_connection()
        self.__ods_product_order_detail = data

    def generate(self):
        self.delete_packs()
        self.stg_packs = self.config.dw
        self.save(self.stg_packs,
                  'ods',
                  'packs',
                  self.config.dw)
        self.dw_ods_product_order_ios = self.config.dw
        self.delete_ods_product_order_ios()
        self.save(
            self.dw_ods_product_order_ios[
                self.select_purchase_ios],
            'ods',
            'product_order_ios',
            self.config.dw
        )
        self.ods_product_order_detail = self.config.dw
        self.delete_ods_product_order_detail()
        self.save(
            self.ods_product_order_detail,
            'ods',
            'product_order_detail',
            self.config.dw
        )
