select
        orderid as order_id,

        -- amt in cents
        amount/100 as amount,
        status

        from raw.stripe.payment