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
    crct.coursenumber AS course_number,
    COALESCE(lit.clid, 0) AS circ_count
FROM
    folio_courses.coursereserves_courses__t crct
LEFT JOIN folio_courses.coursereserves_reserves__t crrt
       ON crct.courselistingid = crrt.courselistingid
LEFT JOIN folio_derived.item_ext iext
       ON crrt.itemid = iext.item_id
LEFT JOIN folio_derived.holdings_ext hrt
       ON iext.holdings_record_id = hrt.holdings_id
LEFT JOIN folio_derived.instance_ext inst
       ON hrt.instance_id = inst.instance_id
LEFT JOIN (
        SELECT 
            item_id,
            COUNT(loan_id) AS clid
        FROM folio_derived.loans_items
        WHERE 
            loan_date::date >= start_date
            AND loan_date::date <= end_date
        GROUP BY item_id
) lit
       ON lit.item_id = crrt.itemid
WHERE 
    crrt.itemid IS NOT NULL
    -- Exclusion Logic
    AND (
        exclusions IS NULL OR (
            (exclusions NOT ILIKE '%POP%' OR crct.coursenumber IS DISTINCT FROM 'POP') AND
            (exclusions NOT ILIKE '%LAW%' OR (crct.coursenumber NOT ILIKE 'Law' AND crct.coursenumber NOT ILIKE 'LAW')) AND
            (exclusions NOT ILIKE '%NEW%' OR crct.coursenumber IS DISTINCT FROM 'NEW') AND
            (exclusions NOT ILIKE '%EMPTY%' OR (crct.coursenumber IS NOT NULL AND crct.coursenumber <> ''))
        )
    )
    -- Course Code Logic
    AND (
        course_codes IS NULL 
        OR course_codes = ''
        OR crct.coursenumber = ANY(string_to_array(course_codes, ','))
    )
GROUP BY 
    iext.barcode, inst.title, crct.coursenumber, lit.clid
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;