-- Проверяем, что NULL в чаевых сохраняется исключительно для наличных (cash),
-- а для безналичной оплаты tip_amount не равен NULL.
select ride_id, payment_method, tip_amount
from {{ ref('rides') }}
where status = 'completed'
  and ((payment_method = 'cash' and tip_amount is not null)
    or (payment_method in ('card', 'wallet') and tip_amount is null))
