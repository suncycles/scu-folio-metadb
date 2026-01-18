--metadb:function circ_stats_course_reserves_all

DROP FUNCTION IF EXISTS circ_stats_course_reserves_all;

CREATE FUNCTION circ_stats_course_reserves_all(
    start_date date DEFAULT '0001-01-01',
    end_date   date DEFAULT '9999-12-31'
    course_codes text DEFAULT NULL,
    exclusions text DEFAULT NULL
)
RETURNS TABLE(
    course_listing_id text,
    course_number text,
    item_id text,
    item_barcode text,
    instance_title text,
    circ_count bigint
)
AS $$
SELECT DISTINCT
    crct.course_listing_id,
    crct.course_number,
    crrt.item_id,
    iext.barcode AS item_barcode,
    inst.title AS instance_title,
    COALESCE(lit.circ_count, 0) AS circ_count
FROM
    folio_courses.coursereserves_courses__t__ crct
INNER JOIN folio_courses.coursereserves_reserves__t__ crrt
       ON crct.course_listing_id = crrt.course_listing_id
LEFT JOIN folio_derived.item_ext iext
       ON crrt.item_id = iext.item_id
LEFT JOIN folio_derived.holdings_ext hrt
       ON iext.holdings_record_id = hrt.holdings_id
LEFT JOIN folio_derived.instance_ext inst
       ON hrt.instance_id = inst.instance_id
LEFT JOIN (
        SELECT 
            item_id,
            COUNT(loan_id) AS circ_count
        FROM folio_derived.loans_items
        WHERE 
            loan_date::date >= start_date
            AND loan_date::date <= end_date
        GROUP BY item_id
) lit
       ON lit.item_id = crrt.item_id
WHERE 
    crrt.item_id IS NOT NULL
    AND (
        exclusions IS NULL OR (
            (exclusions NOT ILIKE '%POP%' OR crct.course_number IS DISTINCT FROM 'POP') AND
            (exclusions NOT ILIKE '%LAW%' OR crct.course_number NOT ILIKE 'LAW%') AND 
            (exclusions NOT ILIKE '%NEW%' OR crct.course_number IS DISTINCT FROM 'NEW') AND
            (exclusions NOT ILIKE '%EMPTY%' OR (crct.course_number IS NOT NULL AND crct.course_number <> ''))
        )
    )
    AND (
        course_codes IS NULL 
        OR course_codes = ''
        OR crct.course_number = ANY(string_to_array(course_codes, ','))
    )
ORDER BY 
    crct.course_number, inst.title
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;