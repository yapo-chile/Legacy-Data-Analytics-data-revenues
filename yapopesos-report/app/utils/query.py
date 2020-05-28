from utils.read_params import ReadParams


class DataWarehouseQuery:
    """
    Class that store Datawarehouse queries
    """
    def __init__(self,
                 params: ReadParams) -> None:
        self.params = params

    def get_product_orders(self) -> str:
        """
        Method return str with query
        """
        query = """
                select
                    inf.*,
                    pp.*
                from
                    (select 
                        user_id_nk,
                        case when product_id_fk = 22 then 'cars' else 'inmo' end category,
                        count(*) insertion_fees,
                        min(payment_date) min_payment_date,
                        max(payment_date) max_payment_date
                    from 
                        ods.product_order po 
                    where 
                        product_id_fk in (22,23) 
                        and status in ('confirmed', 'paid', 'sent', 'failed')
                        and payment_date::date between '{0}' and '{1}'
                    group by 
                        1,2)inf
                left join
                    (select 
                            lister,
                            to_char(payment_date::date, 'YYYY-mm') month_id,
                            sum(price)
                        from 
                            dm.transactions_pp_analysis tpa
                        where 
                            payment_date::date between '{0}' and '{1}'
                        group by 
                            1,2)pp on inf.user_id_nk = pp.lister
                order by 
                    3 desc;
            """.format(self.params.date_from, self.params.date_to)
        return query

    def get_packs(self):
        query = """select 
                    email,
                    category,
                    p.slots,
                    p.date_start,
                    p.date_end,
                    pp.*
                from 
                    ods.packs p
                left join 
                    (select 
                        lister,
                        to_char(payment_date::date, 'YYYY-mm') month_id,
                        sum(price)
                    from 
                        dm.transactions_pp_analysis tpa
                    where 
                        payment_date::date between '{0}' and '{1}'
                    group by 
                        1,2)pp on pp.lister = p.email
                where
                    now() between date_start and date_end""".format(self.params.date_from, self.params.date_to)
        return query

class CreditQuery:
    """
    Class that store Credit db queries
    """
    def __init__(self,
                 params: ReadParams) -> None:
        self.params = params

    def get_credit_total(self) -> str:
        """
        Method return str with query
        """
        query = """
                select 
                    user_id ,
                    sum(credits) credits_buyed,
                    sum(credits-used) credits_available
                from 
                    credits c 
                where
                    expiration_date::date > '{}'
                group by 
                    1;
            """.format(self.params.date_to)
        return query


class BlocketQuery:
    """
    Class that store Credit db queries
    """
    def __init__(self,
                 params: ReadParams) -> None:
        self.params = params

    def get_accounts(self, users) -> str:
        """
        Method return str with query
        """
        query = """
                select 
                    account_id, 
                    email 
                from 
                    accounts a 
                where 
                    account_id in ({})""".format(users)
        return query