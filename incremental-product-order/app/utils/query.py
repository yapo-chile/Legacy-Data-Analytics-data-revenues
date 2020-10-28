from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_truncate_product_order_stg(self) -> str:
        """
        Method return query to truncate stg.product_order
        """
        query = """
                truncate table stg.product_order
            """
        return query

    def query_get_product_order_blocket(self) -> str:
        """
        Method return str with query 
        """
        query = """
        select  ad_id, 
                payg.payment_group_id as payment_id,
                purd.product_id as product_id_nk, 
                pur.purchase_id as product_order_nk,
                pur.receipt as creation_date, 
                pur.receipt as payment_date,
                pur.status as status,
                purd.price,
                email as user_id_nk,
                pur.payment_platform,
                pur.doc_num,
                purd.purchase_detail_id as purchase_detail_id_nk,
                pur.payment_method,
                pur.doc_type
        from    purchase_detail as purd 
        inner join	purchase as pur on purd.purchase_id = pur.purchase_id
        inner join	payment_groups as payg on purd.payment_group_id = payg.payment_group_id
        where   pur.receipt::date between '{date_from}' and '{date_to}'
        union all
        select  ad_id, 
                payg.payment_group_id as payment_id,
                purd.product_id as product_id_nk, 
                pur.purchase_id as product_order_nk,
                pur.receipt as creation_date, 
                pur.receipt as payment_date,
                pur.status as status,
                purd.price,
                email as user_id_nk,
                pur.payment_platform,
                pur.doc_num,
                purd.purchase_detail_id as purchase_detail_id_nk,
                pur.payment_method,
                pur.doc_type
        from 	blocket_{last_year}.purchase_detail as purd
        inner join  blocket_{last_year}.purchase as pur on purd.purchase_id = pur.purchase_id
        inner join	blocket_{last_year}.payment_groups as payg on purd.payment_group_id = payg.payment_group_id
        where   pur.receipt::date between '{date_from}' and '{date_to}'
        union all
        select	ad_id, 
                payg.payment_group_id as payment_id,
                purd.product_id as product_id_nk, 
                pur.purchase_id as product_order_nk,
                pur.receipt as creation_date, 
                pur.receipt as payment_date,
                pur.status as status,
                purd.price,
                email as user_id_nk,
                pur.payment_platform,
                pur.doc_num,
                purd.purchase_detail_id as purchase_detail_id_nk,
                pur.payment_method,
                pur.doc_type
        from 	blocket_{current_year}.purchase_detail as purd 
        inner join  blocket_{current_year}.purchase as pur on purd.purchase_id = pur.purchase_id
        inner join	blocket_{current_year}.payment_groups as payg on purd.payment_group_id = payg.payment_group_id
        where   pur.receipt::date between '{date_from}' and '{date_to}'
        """.format(current_year=self.params.get_current_year(),
                    last_year=self.params.get_last_year(),
                    date_from=self.params.get_date_from(),
                    date_to=self.params.get_date_to())
        return query

    

    def query_delete_product_order_ods(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
        delete from ods.product_order
        where creation_date between '{date_from}' and '{date_to}';            
        """.format(date_from=self.params.get_date_from(),
                    date_to=self.params.get_date_to())

        return command

    def query_get_product_order_stg(self) -> str:
        """
        Method return str with query
        """
        query = """
        SELECT  po.product_id_nk,
                product_order_nk, 
                creation_date,
                payment_date,
                payment_id,
                price, 
                status, 
                ad_id,
                user_id_nk,
                now() as insert_date, 
                payment_platform, 
                (case when p.product_id_nk is null then 0 
                    else p.product_id_pk end) as product_id_fk,
                doc_num,
                purchase_detail_id_nk,
                payment_method,
                doc_type
        FROM stg.product_order po
        left join ods.product p
        on p.product_id_nk = po.product_id_nk

        """
        return query