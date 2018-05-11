
def recommend_pno_by_default(self, return_pno_count):
    cursor = self.wksc_conn.cursor()
    sql_format = '''
            select product_no, spu, sales_volume, favorite, carts
            from qmarket_bigdata.bd_edw_ai_wk_search
            where
            spu is NULL
            or
            2*sales_volume+favorite+carts in (
            select max(2*sales_volume+favorite+carts)
            from qmarket_bigdata.bd_edw_ai_wk_search
            group by spu
            )
            order by 2*sales_volume+favorite+carts desc
            limit %s;
        '''
    cursor.execute(sql_format % return_pno_count)
    pno_list = list()
    for row in cursor.fetchall():
        pno_list.append(row[0])
    return pno_list