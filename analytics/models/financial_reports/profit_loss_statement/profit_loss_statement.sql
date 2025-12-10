{{ config(materialized='view') }}

SELECT name,
       label,
       balance
From {{ ref('profit_loss_ephemeral') }}
