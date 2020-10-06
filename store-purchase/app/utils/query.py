# pylint: disable=no-member
# utf-8


class StorePurchaseQuery:

    def clean_table(self) -> str:
        return """delete from ods.store_purchase where payment_date between 
                    '{DATE_FROM}' and '{DATE_TO}';""" \
                    .format(DATE_FROM=self.params.get_date_from(),
                            DATE_TO=self.params.get_date_to())

    def blocket_store_purchases(self) -> str:
        """
        Method return str with query
        """
        query = """
            select 
                c.store_id as store_id_nk,
                ac.email,
                ac.user_id,
                d.name as store_name,
                d.region,
                (a.receipt)::timestamp as payment_date,
                a.total_price,
                a.doc_type,
                a.payment_method,
                p.name as product_id_nk,
                case when date(a.receipt) < date(e.old_value) then (e.old_value)::timestamp
                    else (a.receipt)::timestamp end as date_start_active,	
                (e.new_value)::timestamp as date_end_active
            from 
                purchase  a
            inner join	
                purchase_detail b on a.purchase_id = b.purchase_id  
            left join
                store_actions c on c.payment_group_id = a.payment_group_id
            inner join
                stores d on d.store_id = c.store_id
            inner join
                accounts ac on ac.account_id = d.account_id
            inner join
                pmnt_api_products p on p.product_id = b.product_id
            left join
                store_changes e on e.store_id = d.store_id and e.action_id = c.action_id and e.column_name = 'date_end'
            where
                b.product_id in (4,5,6,7)
                and state = 'accepted'
                and a.status = 'confirmed'
                and a.receipt > '{DATE_FROM}' and a.receipt < '{DATE_TO}';
            """.format(DATE_FROM=self.params.get_date_from(),
                       DATE_TO=self.params.get_date_to())
        return query
