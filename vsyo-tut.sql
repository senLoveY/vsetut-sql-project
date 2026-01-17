/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Павел Арсенович
 * Дата: 04.10.2025
*/


/* Часть 1. Разработка витрины данных
 * Запрос для создания витрины product_user_features
*/

CREATE MATERIALIZED VIEW product_user_features AS
WITH
base_orders_with_users AS (
  SELECT
    o.order_id,
    o.order_purchase_ts,
    o.order_status,
    u.user_id,
    u.region
  FROM ds_ecom.orders AS o
  INNER JOIN ds_ecom.users AS u
    ON o.buyer_id = u.buyer_id
  WHERE
    o.order_status IN ('Доставлено', 'Отменено') AND u.region IS NOT NULL AND u.user_id IS NOT NULL
),
top_regions AS (
  SELECT
    region
  FROM base_orders_with_users
  GROUP BY region
  ORDER BY COUNT(DISTINCT order_id) DESC
  LIMIT 3
),
order_payment_features AS (
  SELECT
    order_id,
    MAX(CASE WHEN payment_sequential = 1 AND payment_type = 'денежный перевод' THEN 1 ELSE 0 END) AS is_first_payment_money_transfer,
    MAX(CASE WHEN payment_type = 'промокод' THEN 1 ELSE 0 END) AS is_promo_order,
    MAX(CASE WHEN payment_installments > 1 THEN 1 ELSE 0 END) AS is_installment_order
  FROM ds_ecom.order_payments
  GROUP BY order_id
),
order_cost_features AS (
  SELECT
    order_id,
    SUM(price + delivery_cost) AS total_cost
  FROM ds_ecom.order_items
  GROUP BY order_id
),
order_review_features AS (
  SELECT
    order_id,
    review_id,
    CASE
      WHEN review_score > 5 THEN CAST(review_score AS REAL) / 10
      ELSE review_score
    END AS corrected_review_score
  FROM ds_ecom.order_reviews
),
final_data_mart AS (
  SELECT
    b.user_id,
    b.region,
    MIN(b.order_purchase_ts) AS first_order_ts,
    MAX(b.order_purchase_ts) AS last_order_ts,
    (MAX(b.order_purchase_ts) - MIN(b.order_purchase_ts)) AS lifetime,
    COUNT(DISTINCT b.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN b.order_status = 'Отменено' THEN b.order_id END) AS num_canceled_orders,
    AVG(rev.corrected_review_score) AS avg_order_rating,
    COUNT(rev.review_id) AS num_orders_with_rating,
    SUM(CASE WHEN b.order_status = 'Доставлено' THEN cost.total_cost ELSE 0 END) AS total_order_costs,
    AVG(CASE WHEN b.order_status = 'Доставлено' THEN cost.total_cost END) AS avg_order_cost,
    SUM(COALESCE(pay.is_promo_order, 0)) AS num_orders_with_promo,
    SUM(COALESCE(pay.is_installment_order, 0)) AS num_installment_orders,
    MAX(COALESCE(pay.is_first_payment_money_transfer, 0)) AS used_money_transfer,
    MAX(COALESCE(pay.is_installment_order, 0)) AS used_installments,
    MAX(CASE WHEN b.order_status = 'Отменено' THEN 1 ELSE 0 END) AS used_cancel
  FROM base_orders_with_users AS b
  INNER JOIN top_regions AS tr ON b.region = tr.region
  LEFT JOIN order_payment_features AS pay ON b.order_id = pay.order_id
  LEFT JOIN order_cost_features AS cost ON b.order_id = cost.order_id
  LEFT JOIN order_review_features AS rev ON b.order_id = rev.order_id
  GROUP BY
    b.user_id,
    b.region
)
SELECT
  user_id,
  region,
  first_order_ts,
  last_order_ts,
  lifetime,
  total_orders,
  avg_order_rating,
  num_orders_with_rating,
  num_canceled_orders,
  CASE 
    WHEN total_orders > 0 THEN CAST(num_canceled_orders AS REAL) / total_orders 
    ELSE 0 
  END AS canceled_orders_ratio,
  total_order_costs,
  avg_order_cost,
  num_installment_orders,
  num_orders_with_promo,
  used_money_transfer,
  used_installments,
  used_cancel
FROM final_data_mart;




/* Часть 2. Решение ad hoc задач
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/

-- Напишите ваш запрос тут
WITH user_segmentation AS (
	SELECT
    	user_id,
    	total_orders,
    	avg_order_cost,
    	CASE
      		WHEN total_orders = 1 THEN '1 заказ'
      		WHEN total_orders BETWEEN 2 AND 5 THEN '2-5 заказов'
      		WHEN total_orders BETWEEN 6 AND 10 THEN '6-10 заказов'
      		ELSE '11 и более заказов'
    		END AS user_segment
  	FROM
    	ds_ecom.product_user_features
)
	SELECT
  		user_segment,
  		COUNT(DISTINCT user_id) AS users_count,
  		AVG(total_orders)::numeric(4,2) AS avg_orders_per_user,
  		AVG(avg_order_cost) AS avg_order_cost
	FROM
  		user_segmentation
	GROUP BY
  		user_segment
	ORDER BY
  	CASE
    	WHEN user_segment = '1 заказ' THEN 1
    	WHEN user_segment = '2-5 заказов' THEN 2
    	WHEN user_segment = '6-10 заказов' THEN 3
    	ELSE 4
  	END;



/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * 
*/
/*
 * Абсолютное большинство пользователей (60 460 человек) совершили всего один заказ. 
 * Это указывает на большую долю "случайных" покупателей и является ключевой зоной роста для бизнеса — необходимо работать над удержанием и стимулированием повторных покупок.
 * Средняя стоимость заказа снижается по мере увеличения лояльности клиента. 
 * Клиенты, совершившие одну покупку, в среднем тратили больше (3324), чем те, кто совершил 2-5 заказов (3091). 
 * Это может говорить о том, что первая покупка часто бывает крупной ("пробной"), а последующие — более мелкими и регулярными.
*/

/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

-- Напишите ваш запрос тут
SELECT
    user_id,
    total_orders,
    avg_order_cost,
    rank_position
FROM (
    SELECT
        user_id,
        total_orders,
        avg_order_cost,
        RANK() OVER (ORDER BY avg_order_cost DESC) as rank_position
    FROM
        ds_ecom.product_user_features
    WHERE
        total_orders >= 3
) ranked_users
WHERE
    rank_position <= 15
ORDER BY
    rank_position, user_id;


/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * В таблице представлен топ-15 самых ценных клиентов, которые сочетают в себе лояльность (3 и более заказа) и очень высокую покупательскую способность. 
 * Средний чек этих пользователей (от 5 526 до 14 716) в разы превышает средний чек по всей клиентской базе.
 * Удержание этих 15 пользователей должно быть абсолютным приоритетом. 
 * Для них следует разрабатывать персонализированные предложения, предоставлять эксклюзивный сервис и внедрять специальные программы лояльности. 
 * Потеря даже одного такого клиента будет более ощутимой для бизнеса, чем потеря десятков клиентов с одним заказом.
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

-- Напишите ваш запрос тут
SELECT
    region,
    COUNT(DISTINCT user_id) AS total_clients,
    SUM(total_orders) AS total_orders,
    SUM(total_order_costs) / SUM(total_orders) AS avg_order_cost_weighted,
    CAST(SUM(num_installment_orders) AS REAL) / SUM(total_orders) AS installment_orders_ratio,
    CAST(SUM(num_orders_with_promo) AS REAL) / SUM(total_orders) AS promo_orders_ratio,
    CAST(SUM(used_cancel) AS REAL) / COUNT(DISTINCT user_id) AS canceling_users_ratio
FROM
    ds_ecom.product_user_features
GROUP BY
    region
ORDER BY
    total_clients DESC;
/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * Москва является абсолютным лидером по количеству клиентов и заказов, однако уступает другим регионам по среднему чеку. 
 * В Санкт-Петербурге и Новосибирской области средняя стоимость заказа выше, что во многом обусловлено крайне высокой популярностью рассрочки — её используют более чем в 50% заказов. 
 * При этом во всех регионах наблюдается низкая эффективность промокодов и очень малая доля отмен.
*/



/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

-- Напишите ваш запрос тут
SELECT
    to_char(first_order_ts, 'Month') AS first_order_month_name,
    DATE_TRUNC('month', first_order_ts) AS first_order_month,
    COUNT(DISTINCT user_id) AS total_clients,
    SUM(total_orders) AS total_orders,
    AVG(avg_order_cost) AS avg_order_cost,
    AVG(avg_order_rating) AS avg_rating,
    CAST(SUM(used_money_transfer) AS REAL) / COUNT(DISTINCT user_id) AS money_transfer_users_ratio,
    AVG(lifetime) AS avg_lifetime_days
FROM
    ds_ecom.product_user_features
WHERE
    EXTRACT(YEAR FROM first_order_ts) = 2023
GROUP BY
    first_order_month_name, first_order_month
ORDER BY
    first_order_month;

/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * Привлечение новых клиентов значительно выросло к концу 2023 года, достигнув пика в ноябре, что, вероятно, связано с предпраздничными распродажами. 
 * Однако, несмотря на успешное привлечение, "время жизни" новых когорт клиентов резко сократилось: с 13 дней для январских новичков до всего 2 дней для тех, кто пришел в декабре. 
 * Это тревожный сигнал, указывающий на то, что новые пользователи не становятся постоянными покупателями, а совершают лишь одну "разовую" покупку.