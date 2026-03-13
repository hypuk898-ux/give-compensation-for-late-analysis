WITH --отмененные заказы
        cancelled_orders AS (
        SELECT date_trunc('month', "createdDateTime"::date)::date AS created_month,
                id AS order_id
        FROM grp_ods_es00_orhst.order_snp
        WHERE status = 'CANCELLED'
                AND "createdDateTime"::date >= '2025-10-01'
                AND "createdDateTime"::date < '2026-02-01'
),
        --типы отмененных заказов
cancell_type AS ( 
        SELECT c.created_month,
                c.order_id,
                CASE 
                        WHEN orr.TYPE = 'CANCELLED_BY_USER' THEN 'Самостоятельная отмена'
                        WHEN orr.TYPE = 'CANCELLEDBYDELIVERYPARTNER' THEN 'Отмена курьером'
                        WHEN caos."orderId" IS NOT NULL THEN 'Отмена через поддержку'
                        ELSE 'Отмена ЦФЗ' 
                END cancellation_source
        FROM cancelled_orders c
        LEFT JOIN grp_ods_orhst.order_reject_reason_snp orr ON c.order_id::text=orr.order_id::text
        LEFT JOIN grp_ods_es00_operc.canceled_order_snp caos ON c.order_id = caos."orderId"
),
        --заказы с опозданиями
late_orders AS (
        SELECT order_id,
                date_trunc('month', created_dttm::date)::date AS order_month,
                created_dttm + (delivery_sla_min|| ' minute')::INTERVAL AS planned_delivery,
                delivered_dttm
        FROM grp_em.fct_order
        WHERE created_dttm::date >= '2025-10-01'
),
        --опоздания в минутах
late_agg AS (
        SELECT order_id,
                order_month,
                EXTRACT(epoch FROM (delivered_dttm - planned_delivery))/60 AS late_min
        FROM late_orders         
),
        --группировка по времени опоздания
late_minutes AS (
        SELECT count(order_id) AS orders_count,
                CASE 
                        WHEN late_min >= 15 AND late_min <= 30 THEN 'late_15_30_min'
                        WHEN late_min > 30 AND late_min <= 60 THEN 'late_31_60_min'
                        ELSE 'late_more_61_min'
                END AS late_time,
                order_month
        FROM late_agg
        WHERE late_min >= 15
        GROUP BY 2,3
),
late_base AS (
        SELECT order_month,        
                sum(CASE WHEN late_time = 'late_15_30_min'
                                THEN orders_count ELSE 0 END) AS late_15_30_min,
                sum(CASE WHEN late_time = 'late_31_60_min'
                                THEN orders_count ELSE 0 END) AS late_31_60_min,
                sum(CASE WHEN late_time = 'late_more_61_min'
                                THEN orders_count ELSE 0 END) AS late_more_61_min
        FROM late_minutes
        GROUP BY 1
        ORDER BY 1
),
        --отмененные заказы по ЦФЗ
cancell_base AS (
        SELECT created_month,        
                count(DISTINCT(order_id)) AS cfz_cancelled_orders
        FROM cancell_type c 
        WHERE cancellation_source = 'Отмена ЦФЗ'
        GROUP BY 1
        ORDER BY 1
)
SELECT c.created_month,
        c.cfz_cancelled_orders,
        l.late_15_30_min,
        l.late_31_60_min,
        l.late_more_61_min
FROM cancell_base c
LEFT JOIN late_base l ON l.order_month = c.created_month
ORDER BY 1
