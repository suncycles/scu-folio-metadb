--metadb:function _checkout_counts

DROP FUNCTION IF EXISTS _checkout_counts;

CREATE FUNCTION _checkout_counts(
    barcode text
)
RETURNS TABLE(
    item_barcode text,
    loan_date timestamp,
    loan_time time
)
AS $$
SELECT 
    iext.barcode AS item_barcode,
    li.loan_date AS loan_date,
    li.loan_date::time AS loan_time
FROM folio_derived.item_ext iext
INNER JOIN folio_derived.loans_items li
    ON iext.item_id = li.item_id
WHERE 
    iext.barcode = $1
ORDER BY 
    li.loan_date DESC
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;
