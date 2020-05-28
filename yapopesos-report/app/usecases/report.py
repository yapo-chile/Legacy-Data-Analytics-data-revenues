import pandas as pd
from utils.query import DataWarehouseQuery, CreditQuery, BlocketQuery
from utils.read_params import ReadParams
from infraestructure.psql import Database


class Report():
    def __init__(self, config, params: ReadParams) -> None:
        self.config = config
        self.params = params

    @property
    def dwh_data(self):
        return self.__dwh_data

    @dwh_data.setter
    def dwh_data(self, config):
        def to_int(x):
            try:
                return int(x)
            except:
                return x
        query = DataWarehouseQuery(self.params).get_product_orders()
        db = Database(config)
        data = db.select_to_dict(query)
        data['sum'] = data['sum'].map(lambda x: to_int(x))
        data['sum'] = data['sum'].fillna(0).astype(int)
        self.__dwh_data = data.groupby(['user_id_nk',
                                        'category',
                                        'insertion_fees',
                                        'min_payment_date',
                                        'max_payment_date',
                                        'month_id']) \
                                .agg({'sum': 'sum' }).unstack().fillna(0)
        self.__dwh_data = self.__dwh_data.astype(int)
        column_names = list()
        self.__dwh_data.reset_index(inplace=True)
        for i in self.__dwh_data.columns:
            column_names.append(i[0]+'_'+i[1])
        self.__dwh_data.columns = column_names
        db.close_connection()
    
    @property
    def credit_data(self):
        return self.__credit_data

    @credit_data.setter
    def credit_data(self, config):
        query = CreditQuery(self.params).get_credit_total()
        db = Database(config)
        self.__credit_data = db.select_to_dict(query)
        db.close_connection()

    @property
    def pack_data(self):
        return self.__pack_data

    @pack_data.setter
    def pack_data(self, config):
        query = DataWarehouseQuery(self.params).get_packs()
        db = Database(config)
        data = db.select_to_dict(query)
        self.__pack_data = data.groupby(['email',
            'category',
            'slots',
            'date_start',
            'date_end',
            'month_id']).agg({'sum':'sum'}).unstack().fillna(0)
        self.__pack_data = self.__pack_data.astype(int)
        column_names = list()
        self.__pack_data.reset_index(inplace=True)
        for i in self.__pack_data.columns:
            column_names.append(i[0]+'_'+i[1])
        self.__pack_data.columns = column_names
        db.close_connection()

    @property
    def account_data(self):
        return self.__account_data

    @account_data.setter
    def account_data(self, config):
        accounts = self.credit_data['user_id'].tolist()
        query = BlocketQuery(self.params).get_accounts(','.join([str(x) for x in accounts]))
        db = Database(config)
        self.__account_data = db.select_to_dict(query)
        db.close_connection()

    def generate(self):
        self.dwh_data = self.config.db
        self.credit_data = self.config.credit
        self.account_data = self.config.blocket
        self.pack_data = self.config.db

        credits_df = self.credit_data.merge(self.account_data, 
                                            left_on='user_id', 
                                            right_on='account_id', 
                                            how='inner')
        #First sheet of report
        insfee_with_pp_expense = self.dwh_data.merge(credits_df, left_on='user_id_nk_', right_on='email', how='left').fillna(0)
        
        # Second sheet of report
        active_packs = self.pack_data.merge(credits_df, left_on='email_', right_on='email', how='left').fillna(0)
        active_packs['credits_buyed'] = active_packs['credits_buyed'].astype(int)
        active_packs['credits_available'] = active_packs['credits_available'].astype(int)
        with pd.ExcelWriter('output.xlsx') as writer:  
            insfee_with_pp_expense.to_excel(writer, sheet_name='insfee_with_pp_expense')
            active_packs.to_excel(writer, sheet_name='active_packs_with_pp_expense')
        return True
