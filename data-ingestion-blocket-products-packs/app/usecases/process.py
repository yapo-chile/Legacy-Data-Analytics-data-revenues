# pylint: disable=no-member
# utf-8
import logging
from usecases.blocket_packs import BlocketPacks
from usecases.blocket_puchases import BlocketPuchases
from usecases.ods_blocket import OdsBlocket


class Process:
    def __init__(self, config, params) -> None:
        self.config = config
        self.params = params
        self.logger = logging.getLogger('etl-blocket')

    def generate(self):
        self.logger.info('Starting process of data ingestion to DW')
        BlocketPacks(self.config, self.params).generate()
        BlocketPuchases(self.config, self.params).generate()
        self.logger.info('Iniciando ODS BLOCKET')
        OdsBlocket(self.config, self.params).generate()
        self.logger.info('Ending process of data ingestion')
