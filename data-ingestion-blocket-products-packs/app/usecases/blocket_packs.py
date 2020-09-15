# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import Query


class BlocketPacks():
    # pylint: disable=R0902
    def __init__(self, config, params) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('BlocketPacks')

    def save(self, data, schema, table_name, configdb) -> None:
        db = Database(conf=configdb)
        self.logger.info('Iniciando inserción de datos')
        db.insert_copy(schema, table_name, data)
        self.logger.info(
            'Datos insertados en {}.{}'.format(schema, table_name))
        db.close_connection()

    def truncate_stg_pack_autos(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Truncando stg.pack_autos')
        db.execute_command(query.tr_stg_pack_autos())
        db.close_connection()

    # Query data from data warehouse
    @property
    def stg_pack_autos(self):
        return self.__stg_pack_autos

    @stg_pack_autos.setter
    def stg_pack_autos(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.stg_pack_autos())
        db_source.close_connection()
        self.__stg_pack_autos = data_dwh

    def truncate_pack_manual_acepted(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Truncando stg.pack_manual_acepted')
        db.execute_command(query.tr_pack_manual_acepted())
        db.close_connection()

    @property
    def pack_manual_acepted(self):
        return self.__pack_manual_acepted

    @pack_manual_acepted.setter
    def pack_manual_acepted(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.pack_manual_acepted())
        db_source.close_connection()
        self.__pack_manual_acepted = data_dwh

    def delete_ads_disabled_pack_autos(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Delete de stg.pack_manual_acepted')
        db.execute_command(query.del_ads_disabled_pack_autos())
        db.close_connection()

    @property
    def ads_disabled_pack_autos(self):
        return self.__ads_disabled_pack_autos

    @ads_disabled_pack_autos.setter
    def ads_disabled_pack_autos(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.ads_disabled_pack_autos())
        db_source.close_connection()
        self.__ads_disabled_pack_autos = data_dwh

    def delete_packs(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dw)
        self.logger.info('Delete de stg.packs')
        db.execute_command(query.del_stg_packs())
        db.close_connection()

    @property
    def packs(self):
        return self.__packs

    @packs.setter
    def packs(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(
            query.packs())
        db_source.close_connection()
        self.__packs = data_dwh

    def generate(self):
        self.stg_pack_autos = self.config.db
        self.truncate_stg_pack_autos()
        self.save(self.stg_pack_autos,
                  'stg',
                  'temp_pack',
                  self.config.dw)
        self.pack_manual_acepted = self.config.db
        self.truncate_pack_manual_acepted()
        self.save(self.pack_manual_acepted,
                  'stg',
                  'pack_manual_accepted',
                  self.config.dw)
        self.ads_disabled_pack_autos = self.config.db
        self.delete_ads_disabled_pack_autos()
        self.save(self.ads_disabled_pack_autos,
                  'stg',
                  'ads_disabled_pack_autos',
                  self.config.dw)
        self.packs = self.config.db
        self.delete_packs()
        self.save(self.packs,
                  'stg',
                  'packs',
                  self.config.dw)
