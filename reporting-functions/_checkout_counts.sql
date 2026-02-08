--metadb:function _checkout_counts

DROP FUNCTION IF EXISTS _checkout_counts;

CREATE FUNCTION _checkout_counts(
    barcode text
)
RETURNS TABLE(
    item_barcode text,
    loan_date timestamp,
    loan_time text
)
AS $$
SELECT 
    iext.barcode AS item_barcode,
    li.__start AS loan_date,
    li.__start::time::text AS loan_time
FROM folio_derived.item_ext iext
INNER JOIN folio_circulation.loan__t__ li
    ON iext.item_id = li.item_id
WHERE 
    iext.barcode = $1
ORDER BY 
    li.__start DESC
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;