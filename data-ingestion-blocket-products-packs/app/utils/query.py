from datetime import timedelta
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

    def tr_stg_pack_autos(self) -> str:
        return "truncate table stg.temp_pack"

    def stg_pack_autos(self) -> str:
        """
        Method return str that make query to Blocket db
        """
        return """
        select ad_id, 
            purd.price, 
            prod.name as product_id_nk, 
            pur.receipt as creation_date, 
            payg.status, 
            payg.added_at as payment_date,  
            payg.payment_group_id as payment_id
        from pmnt_api_products as prod
        inner join purchase_detail as purd on  purd.product_id = prod.product_id
        inner join purchase as pur on purd.purchase_id = pur.purchase_id
        inner join payment_groups as payg on pur.payment_group_id = payg.payment_group_id
        where pur.receipt::date = '{date_from}'::date
            and	prod.name like '%PC%'
            and payg.status = 'paid'
        """.format(date_from=self.params.date_from)

    def tr_pack_manual_acepted(self) -> str:
        return "truncate table stg.pack_manual_accepted"

    def pack_manual_acepted(self) -> str:
        return """
        select p.pack_id,
            p.date_start,
            p.date_end,
            p.product_id,
            ac.email as user_mail,
            a.fullname as admin_fullname
        from packs p
        inner join tokens t on p.token_id = t.token_id
        inner join admins a on a.admin_id = t.admin_id
        inner join accounts ac on ac.account_id = p.account_id
        where p.date_start::date = '{date_from}'::date
        group by 1,2,3,4,5,6
        """.format(date_from=self.params.date_from)

    def del_ads_disabled_pack_autos(self) -> str:
        date_from = self.params.date_from + timedelta(days=1)
        return """
        delete from stg.ads_disabled_pack_autos
        where now::date = '{date_from}'
        """.format(date_from=date_from)

    def ads_disabled_pack_autos(self) -> str:
        return """
        select now()::date,
            a.ad_id as ad_id_nk,
            a.status
        from ads a
        where a.status = 'disabled'
        group by 1,2,3 """

    def del_stg_packs(self) -> str:
        return """
        delete from stg.packs
        where date_start::date = '{date_from}'
        """.format(date_from=self.params.date_from)

    def packs(self) -> str:
        return """
        select account_id,
            category,
            date_start,
            date_end,
            days,
            slots,
            product_id,
            case when price is null then monto::int * 1.19 else price end as price,
            case when doc_num is null then orden_servicio::float else doc_num end as doc_num,
            tipo_pack,
            email
        from (
            select p.pack_id,
                p.account_id,
                p."type" as category,
                p.date_start,
                p.date_end,
                p.date_end::date - p.date_start::date as days,
                p.slots as slots,
                p.product_id,
                p.payment_group_id,
                pd.price,
                pps."value" as orden_servicio,
                ppa."value" as monto,
                case when p.token_id is not null then 'Pack Manual' else 'Pack Online' end as tipo_pack,
                pu.doc_num,
                pu.status,
                ac.email
            from packs p
            left join payment_groups pg on (p.payment_group_id = pg.parent_payment_group_id or p.payment_group_id = pg.payment_group_id)
            left join purchase pu on pu.payment_group_id = pg.payment_group_id or pu.payment_group_id = pg.parent_payment_group_id
            left join purchase_detail pd on pd.purchase_id = pu.purchase_id and (pd.payment_group_id =  pg.payment_group_id)
            left join pack_params pps on pps.pack_id = p.pack_id and pps."name" = 'service_order'
            left join pack_params ppa on ppa.pack_id = p.pack_id and ppa."name" = 'service_amount'
            left join accounts ac on ac.account_id = p.account_id
            where p.date_start::date = '{date_from}'::date
                and (pu.status in ('confirmed', 'paid', 'sent') or pps.pack_id is not null)
        )t
        """.format(date_from=self.params.date_from)

    #jb_blocket_stg_puchase
    def tr_stg_purchase_ios(self) -> str:
        return "truncate table stg.purchase_ios"

    def stg_purchase_ios(self) -> str:
        return """
        select pia.purchase_in_app_id as purchase_in_app_id_nk,
            pia.receipt_date,
            pia.price,
            pia.status,
            pia.product_id,
            pia.product_name,
            pia.ad_id,
            pia.email,
            pia.payment_platform
        from public.purchase_in_app pia
        where pia.receipt_date::date = '{date_from}'::date
        """.format(date_from=self.params.date_from)

    def tr_product_order_detail(self) -> str:
        return "truncate table stg.product_order_detail"

    def product_order_detail(self) -> str:
        return """
        select a.purchase_detail_id as purchase_detail_id_nk,
            pdp2.value::int as num_days,
            pdp3.value::int as total_bump,
            pdp4.value::int as frequency
        from (--a
            select pdp.purchase_detail_id
            from purchase pur
            inner join purchase_detail pd on pur.purchase_id = pd.purchase_id
            inner join purchase_detail_params pdp on pdp.purchase_detail_id = pd.purchase_detail_id
            where pur.receipt::date = '{date_from}'::date
            group by 1
        )a	
        left join purchase_detail_params pdp2 on a.purchase_detail_id = pdp2.purchase_detail_id and pdp2."name" = 'num_days'
        left join purchase_detail_params pdp3 on a.purchase_detail_id = pdp3.purchase_detail_id and pdp3."name" = 'total_bump'
        left join purchase_detail_params pdp4 on a.purchase_detail_id = pdp4.purchase_detail_id and pdp4."name" = 'frequency'
        """.format(date_from=self.params.date_from)

    def delete_ods_packs(self):
        return """
        DELETE FROM ods.packs
        where pack_id in (
            SELECT pack_id
            FROM stg.packs p
            where
            p.date_start::date = '{date_from}'::date
        )""".format(date_from=self.params.date_from)

    def stg_packs(self):
        return """
        select p.*,
            s.seller_id_pk as seller_id_fk
        from stg.packs p
        left join ods.seller s on p.email = s.email
        where p.date_start::date = '{date_from}'::date
        """.format(date_from=self.params.date_from)

    def del_ods_product_order_ios(self):
        date_from = self.params.date_from + timedelta(days=1)
        return """
        DELETE FROM ods.product_order_ios
        where insert_date::date = '{date_from}'::date
        """.format(date_from=date_from)

    def dw_ods_product_order_ios(self):
        return """
        select pia.purchase_in_app_id_nk as product_order_nk,
            pia.receipt_date as creation_date,
            pia.receipt_date as payment_date,
            pia.price::decimal,
            pia.status,
            now() as insert_date,
            pia.product_id as product_id_nk,
            pia.ad_id as ad_id_nk,
            pia.email as user_id_nk,
            pia.payment_platform,
            case when pia.price > 300 then pia.price else round(pia.price*b.precio_dolar) end as price_clp
        from stg.purchase_ios as pia
        left join (--b obtener el valor del dolar para hacer la conversion
            select month_id,
                case when precio_dolar_sgte is null then precio_dolar else precio_dolar_sgte end as precio_dolar
            from (--d
                select extract('year' from fecha::date)*100 + extract('month' from fecha::date) as month_id,
                    precio_dolar,
                    precio_euro,
                    lag(precio_dolar, -1) over(order by extract('year' from fecha::date)*100 + extract('month' from fecha::date) asc) as precio_dolar_sgte
                from stg.dolar_euro
            )d
        ) b on b.month_id = extract('year' from pia.receipt_date::date) * 100 + extract('month' from pia.receipt_date::date);
        """

    def del_ods_product_order_detail(self) -> str:
        return """
        DELETE from ods.product_order_detail
        where purchase_detail_id_nk in (
        select purchase_detail_id_nk
        from stg.product_order_detail
        )
        """

    def ods_product_order_detail(self) -> str:
        return """
        select *
        from stg.product_order_detail
        """