# -*- coding: utf-8 -*-

import db
import datetime
import decimal


SQL_payment_select = '''SELECT * FROM `payments`
where number = %s
'''
SQL_payment_insert = '''INSERT INTO `payments`
(`number`, `amount`, `description`, `type`,
`status`, `land_id`, `show`, `created_at`, `updated_at`,
`balance_start`, `balance_end`)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''
SQL_payment_update = '''UPDATE `payments`
set amount = %s,
land_id = %s,
created_at = %s,
updated_at = %s       
where number = %s
'''
SQL_land_periods_cursor = '''
SELECT 
a.id,
a.land_id,
ADDDATE( date( a.created_at ) , 1 - day( a.created_at )) period,
a.amount,
a.start_balance,
a.end_balance, 
m.accrued calc_accrued,
p.amount calc_payed        
FROM `accruals` a
left join (
	select 
    group_id,
    land_id,
    sum(accrued) accrued
    from meters
   	group by group_id, land_id
) m on m.group_id = a.id and m.land_id=a.land_id
left join (
	select 
	case when ADDDATE( date( created_at ) , 1 - day( created_at ) ) > coalesce(a1.period, date('1900-01-01')) then a1.period
	else ADDDATE( date( created_at ) , 1 - day( created_at ) )
	end period,
    p1.land_id,
    sum(amount) amount
    from payments p1
    left join (
    	select
    	land_id,
    	max(ADDDATE(date( created_at ) , 1 - day( created_at ))) period
    	from accruals
    	group by land_id
    ) a1 on a1.land_id=p1.land_id
    where status = 'accept'
   	group by case when ADDDATE( date( created_at ) , 1 - day( created_at ) ) > coalesce(a1.period, date('1900-01-01')) then a1.period
	else ADDDATE( date( created_at ) , 1 - day( created_at ) )
	end, land_id
) p on p.period = ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) and p.land_id=a.land_id
where a.land_id = %s
order by a.created_at, id
'''
SQL_accruals_insert = '''INSERT INTO accruals 
(land_id, date, amount, start_balance, end_balance, created_at, updated_at) 
VALUES 
(%s, %s, %s, %s, %s, %s, %s)
'''
SQL_accruals_update = '''UPDATE accruals
set start_balance = %s,
end_balance = %s,
amount = %s,
updated_at = %s       
where id = %s
'''
SQL_land_payments_cursor = '''SELECT p.*, 
ADDDATE( date( p.created_at ) , 1 - day( p.created_at ) ) period,
(
    select 
    a.end_balance
    from accruals a 
    where a.land_id = p.land_id
    and ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) < ADDDATE( date( p.created_at ) , 1 - day( p.created_at ) )
    order by created_at desc
    limit 1
) period_balance
FROM `payments` p
where status = 'accept' and land_id = %s
order by p.created_at, p.id
'''
SQL_payment_balance_update = '''UPDATE `payments`
set balance_start = %s,
balance_end = %s,
updated_at = %s       
where id = %s
'''
SQL_add_lost_coupons = '''
INSERT INTO payments
(`number`, amount, description, `type`, status, land_id, `show`, created_at, updated_at)
select c.`number`,
c.amount,
concat_ws(': ', 'Купон', c.comment),
'coupon',
c.status,
c.land_id,
0,
c.created_at,
now()
from coupons c 
left join payments p on p.`number` = c.`number`
where p.id is null
'''
SQL_accural_doubles = '''
create table tmp_accruals as 
select 
a.id
from accruals a 
join (
	select ADDDATE( date( a.created_at ) , 1 - day( a.created_at )) period,
	land_id,
	min(id) id, 
	count(*)
	from accruals a
	group by ADDDATE( date( a.created_at ) , 1 - day( a.created_at )), land_id
	having count(*) > 1
) ag on ag.period=ADDDATE( date( a.created_at ) , 1 - day( a.created_at )) and ag.land_id = a.land_id
where not a.id = ag.id
order by ADDDATE( date( a.created_at ) , 1 - day( a.created_at )),
a.land_id,
a.id
'''
SQL_accural_doubles_erase = '''
delete from accruals
where id in (select id from tmp_accruals)
'''
SQL_accural_doubles_drop_tmp = '''
drop table tmp_accruals
'''
SQL_lands = '''
select id from lands 
order by id
'''
SQL_payments_by_land_period = '''
'''


class Payment:

    def __init__(self, data):
        self.data = data
        self.db = db.Db()
        self.db.transaction()
        self.date = self.data.get('date', datetime.datetime.now().isoformat())
        self.default = {
            'description': 'Загружено из 1С',
            'type': 'bank',
            'status': 'accept',
            'show': 1,
            'created_at': self.date,
            'updated_at': datetime.datetime.now().isoformat(),
            'balance_start': None,
            'balance_end': None
        }
        self.id = None
        self.number = self.data.get('number')
        self.amount = decimal.Decimal(self.data.get('amount', 0))
        self.land_id = self.data.get('land_id')
        self.current = None
        self.notes = {}

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def exists(self):
        result = self.db.execute(SQL_payment_select, (self.number,))
        if not result:
            return False
        if isinstance(result, list):
            self.current = result[0]
        else:
            self.current = result
        return True

    def update_payment(self):
        self.db.execute(SQL_payment_update, (self.data.get('amount', 0), self.land_id, self.date, self.default['updated_at'],
                                             self.number)
                        )
        self.notes['result'] = 'updated record ID:%s' % (
            self.current.get('id'),)

    def insert_payment(self):
        self.db.execute(SQL_payment_insert, (self.number, self.data.get('amount', 0), self.default['description'], self.default['type'],
                                             self.default['status'], self.land_id, self.default[
                                                 'show'], self.date, self.default['updated_at'],
                                             self.default['balance_start'], self.default['balance_end'])
                        )
        self.notes['result'] = 'inserted record'

    def upsert_payment(self):
        if self.exists():
            self.update_payment()
        else:
            self.insert_payment()


def upsert_payment(data):
    result = []
    for row in data:
        payment = Payment(row)
        payment.upsert_payment()
        result.append({
            'number': row['number'],
            'result': 'added',
            'notes': payment.notes
        })
        # payment.update_balances()
        payment.commit()
    maintenanceAccurals()
    return result


def compactProfile(data):
    result = []
    if not data:
        return result
    if not len(data):
        return result
    profiles = list(
        set([(x.get('pid'), x.get('io_name'), x.get('last_name')) for x in data]))
    if not profiles:
        return result
    permissions = list(set([x.get('permission') for x in data]))
    lands = [{'lid': x[0], 'land_number': x[1], 'address': x[2]} for x in list(
        set([(x.get('lid'), x.get('land_number'), x.get('address')) for x in data]))]
    result.append({
        'profile': {
            'pid': profiles[0][0],
            'io_name': profiles[0][1],
            'last_name': profiles[0][2]
        },
        'permissions': permissions,
        'lands': lands
    })
    return result


def update_accrual_balance(db, land):
    def right_balance(period, balance, end_balance, amount):
        return (balance == period['start_balance']) \
            and (end_balance == period['end_balance']) \
            and (amount == period['amount'])

    def update_accruals(period, balance, end_balance, amount):
        db.execute(SQL_accruals_update, ("%0.2f" % (balance,), "%0.2f" % (end_balance,), "%0.2f" % (amount,), datetime.datetime.now().isoformat(),
                                         period['id'])
                   )

    periods = db.fetch(SQL_land_periods_cursor, (land.get('id')))
    balance = None
    old_balance = 0
    for period in periods:
        # balance = balance or decimal.Decimal(period.get('start_balance', 0) or 0)
        balance = old_balance
        amount = decimal.Decimal(period.get('calc_accrued', 0) or 0)
        end_balance = balance - amount + \
            decimal.Decimal(period.get('calc_payed', 0) or 0)
        if not right_balance(period, balance, end_balance, amount):
            land['status'] = 'updated'
            if not land.get('periods'):
                land['periods'] = []
            land['periods'].append({
                'period': period['period'],
                'old': {
                    'start': "%0.2f" % (period['start_balance'],),
                    'end': "%0.2f" % (period['end_balance'],),
                    'accrued': "%0.2f" % (period['amount'],)
                },
                'new': {
                    'start': "%0.2f" % (balance or 0,),
                    'end': "%0.2f" % (end_balance or 0,),
                    'accrued': "%0.2f" % (amount or 0,),
                    'payed': "%0.2f" % (period.get('calc_payed', 0) or 0,)
                }
            })
            update_accruals(period, balance, end_balance, decimal.Decimal(
                period.get('calc_accrued', 0) or 0))
        balance = end_balance
        old_balance = end_balance
    # print(land)
    return land


def maintenanceAccurals():
    result = []
    mydb = db.Db()
    mydb.transaction()
    try:
        mydb.execute(SQL_accural_doubles_drop_tmp)
        result.append({'action': 'drop temp table'})
    except (Exception):
        pass
    try:
        mydb.execute(SQL_accural_doubles)
        result.append({'action': 'create temp table with doubles'})
        mydb.execute(SQL_accural_doubles_erase)
        result.append({'action': 'delete accruel doubles'})
        mydb.execute(SQL_accural_doubles_drop_tmp)
        result.append({'action': 'drop temp table'})
        mydb.execute(SQL_add_lost_coupons)
    except (Exception):
        pass
    result.append({'action': 'add lost coupons'})
    lands = mydb.fetch(SQL_lands)
    balances = {'action': 'fix balances', 'lands': []}
    for land in lands:
        balances['lands'].append(update_accrual_balance(mydb, land))
    result.append(balances)
    mydb.commit()
    return(result)
