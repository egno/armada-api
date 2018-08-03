SQL = {

    'lands_by_phone': '''select
p.id profile_id,
l.id land_id, l.address, l.area,
a1.date,
a1.end_balance
from profiles p
left join lands l
    on l.profile_id = p.id
left join (
select
a.land_id,
a.date,
a.end_balance
from (
SELECT 
land_id,
max(created_at) created_at
FROM `accruals`
group by land_id
)q
join accruals a
on a.land_id=q.land_id and a.created_at = q.created_at
) a1 on a1.land_id=l.id
where (Length(%(phone)s > 11))
and ( right(%(phone)s, 11) = p.phone or right(%(phone)s, 11) = p.phone2 ) 
order by p.account_number''',

    'periods_by_phone': '''select
p.id profile_id,
l.id land_id, l.address, l.area,
ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) period,
a.start_balance,
a.amount,
a.end_balance
from profiles p
left join lands l
    on l.profile_id = p.id
join accruals a
	on a.land_id=l.id
where (Length(%(phone)s) > 11) 
and ( right(%(phone)s, 11) = p.phone or right(%(phone)s, 11) = p.phone2 ) 
order by a.created_at desc, p.account_number''',

    'period_by_phone': '''select
p.id profile_id,
l.address, l.area,
l.number land_number, 
ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) period, 
ADDDATE( date( m.created_at ) , 1 - day( m.created_at ) ) calc_period, 
ADDDATE( date( m.period ) , 1 - day( m.period ) ) fact_period,
m.service_id,
s.name service_name, 
m.tariff, 
m.accrued
from profiles p
left join lands l
    on l.profile_id = p.id
left join accruals a on a.land_id=l.id
left join meters m on m.group_id = a.id
LEFT JOIN services s ON s.id = m.service_id
where (Length(%(phone)s) > 11) 
and ( right(%(phone)s, 11) = p.phone or right(%(phone)s, 11) = p.phone2 ) 
and ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) = %(period)s
order by l.number, m.service_id, fact_period''',

    'periods_by_land': '''select
a.land_id,
ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) period,
a.start_balance,
a.amount,
m.accrued,
p.amount payed,
a.end_balance
from accruals a
left join (
	select 
    group_id,
    sum(accrued) accrued
    from meters m
    group by group_id
) m on m.group_id = a.id
left join (
	select 
    sum(amount) amount
    from payments p
    where p.status = 'accept'
    and ADDDATE( date( p.created_at ) , 1 - day( p.created_at ) ) = %(period)s
) p on 1=1
where a.land_id = %(land)s
and ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) = %(period)s
''',


    'charge_by_land': '''SELECT 
ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) period, 
ADDDATE( date( m.created_at ) , 1 - day( m.created_at ) ) calc_period, 
ADDDATE( date( m.period ) , 1 - day( m.period ) ) fact_period,
l.number land_number, 
m.service_id,
s.name service_name, 
m.tariff, 
m.accrued
FROM lands l
left join accruals a on a.land_id=l.id
left join meters m on m.group_id = a.id
LEFT JOIN profiles p ON p.id = l.profile_id
LEFT JOIN services s ON s.id = m.service_id
WHERE ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) = %(period)s
and l.number = %(land)s
order by l.number, service_id''',

    'payment_by_land': '''SELECT 
ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) period, 
l.number land_number, 
p.amount,
p.number,
date(p.created_at) created_at
FROM lands l
join accruals a on a.land_id=l.id
join payments p on p.land_id=l.id
    and p.status = 'accept'
    and ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) = ADDDATE( date( p.created_at ) , 1 - day( p.created_at ) )
WHERE ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) = %(period)s
and l.number = %(land)s
order by l.number, p.created_at''',

    'charge_in_period': '''SELECT 
ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) period, 
l.number land_number,  
m.service_id,
s.name service_name, 
m.tariff, 
m.accrued
FROM lands l
join accruals a on a.land_id=l.id
left join meters m on m.group_id = a.id
LEFT JOIN profiles p ON p.id = l.profile_id
LEFT JOIN services s ON s.id = m.service_id
WHERE ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) = %(period)s
order by l.number, service_id''',

    'check_all_balances': '''select
a.land_id,
ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) period,
a.start_balance,
a.amount amount,
m.accrued charged,
p.amount payed,
a.end_balance,
a.start_balance - IFNULL(m.accrued,0) + IFNULL(p.amount,0) - a.end_balance control
from accruals a
left join (
	select 
    group_id,
    sum(accrued) accrued
    from meters m
    group by group_id
) m on m.group_id = a.id
left join (
	select 
    ADDDATE( date( p.created_at ) , 1 - day( p.created_at ) ) period,
    land_id,
    sum(amount) amount
    from payments p
    where
    p.status = 'accept'
    group by land_id, ADDDATE( date( p.created_at ) , 1 - day( p.created_at ) )
) p on p.land_id = a.land_id and p.period = ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ) 
where 
not (a.start_balance - IFNULL(m.accrued,0) + IFNULL(p.amount,0) - a.end_balance = 0)
order by ADDDATE( date( a.created_at ) , 1 - day( a.created_at ) ), a.land_id
''',

    'user_info_by_phone': '''SELECT distinct
p.id pid,
u.id uid,
p.last_name,
concat_ws(' ', p.first_name,
p.father_name) io_name,
per.name permission,
l.id lid,
l.`number` land_number,
l.address address
from profiles p 
left join users u on u.id=p.uid 
left join model_has_roles ur on ur.model_id = u.id
left join roles r on r.id=ur.role_id
left join role_has_permissions rp on rp.role_id=r.id
left join permissions per on per.id=rp.permission_id
left join lands l on l.profile_id = p.id
where ( right(%(phone)s, 11) = p.phone or right(%(phone)s, 11) = p.phone2 )
and u.active = 1
and u.status = 1
''',

    'users_for_sms': '''
SELECT distinct
p.id pid,
u.id uid,
p.last_name,
concat_ws(' ', p.first_name,
p.father_name) io_name,
case when p.phone like '79%' then p.phone
else nullif(trim(p.phone2),'') 
end phone,
u.email
from profiles p 
join users u on u.id=p.uid 
join lands l on l.profile_id = p.id
where u.status = 1
and u.active =1
and case when p.phone like '79%' then p.phone
else nullif(trim(p.phone2),'') 
end is not null'''
}
