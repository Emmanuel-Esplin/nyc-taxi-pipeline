-- dim_payment_type: codes from the TLC data dictionary (seeds/payment_types.csv).
select payment_type, payment_description
from {{ ref('payment_types') }}
