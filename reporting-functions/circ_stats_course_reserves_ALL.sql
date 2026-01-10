--metadb:function circ_stats_course_reserves_all

DROP FUNCTION IF EXISTS circ_stats_course_reserves_all;

CREATE FUNCTION circ_stats_course_reserves_all(
    start_date date DEFAULT '1900-01-01',
    end_date   date DEFAULT '2050-01-01',
    course_codes text DEFAULT NULL,
    exclusions text DEFAULT NULL
)
RETURNS TABLE(
    item_barcode text,
    instance_title text,
    course_number text,
    circ_count numeric
)
AS $$
SELECT 
    iext.barcode AS item_barcode,
    inst.title AS instance_title,
    crct.course_number AS course_number,
    COALESCE(lit.clid, 0) AS circ_count
FROM
    folio_courses.coursereserves_courses__t__ crct
LEFT JOIN folio_courses.coursereserves_reserves__t__ crrt
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
            COUNT(loan_id) AS clid -- Changed back to loan_id
        FROM folio_derived.loans_items
        WHERE 
            (loan_date::date >= start_date OR start_date IS NULL)
            AND (loan_date::date <= end_date OR end_date IS NULL)
        GROUP BY item_id
) lit
       ON lit.item_id = crrt.item_id
WHERE 
    crrt.item_id IS NOT NULL
    -- Exclusion Logic
    AND (
        exclusions IS NULL OR (
            (NOT (exclusions ILIKE '%POP%') OR (crct.course_number IS DISTINCT FROM 'POP')) AND
            (NOT (exclusions ILIKE '%LAW%') OR (crct.course_number NOT ILIKE 'Law' AND crct.course_number NOT ILIKE 'LAW')) AND
            (NOT (exclusions ILIKE '%NEW%') OR (crct.course_number IS DISTINCT FROM 'NEW')) AND
            (NOT (exclusions ILIKE '%EMPTY%') OR (crct.course_number IS NOT NULL AND crct.course_number <> ''))
        )
    )
    -- Course Code Logic
    AND (
        course_codes IS NULL 
        OR course_codes = ''
        OR crct.course_number = ANY(string_to_array(replace(course_codes, ', ', ','), ','))
    )
GROUP BY 
    iext.barcode, inst.title, crct.course_number, lit.clid
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;