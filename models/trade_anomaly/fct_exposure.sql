select
  symbol_order.pnl_risk_usd as open_exposure_usd,
  symbol_order.symbol as symbol,
  account.name as account_name,
  account.zenith_account_id__c as account_id
from {{ ref('int_symbol_order_5min') }} symbol_order
inner join {{ source('PUBLIC', 'SALESFORCE_ACCOUNT' )}} account
  on account.zenith_account_id__c = symbol_order.account_id
inner join {{ source('PUBLIC', 'WATCHLIST') }} watchlist
  on watchlist.entity_name = account.name