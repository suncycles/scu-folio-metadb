--metadb:function _filter_course_reserves_stats

DROP FUNCTION IF EXISTS _filter_course_reserves_stats;

CREATE FUNCTION _filter_course_reserves_stats(
    start_date date DEFAULT '0001-01-01',
    end_date   date DEFAULT '9999-12-31',
    term_name text DEFAULT NULL,
    course_codes text DEFAULT NULL,
    exclusions text DEFAULT NULL,
    show_historical_data text DEFAULT NULL
)
RETURNS TABLE(
    course_number text,
    item_barcode text,
    call_number text,
    instance_title text,
    circ_count bigint,
    is_current boolean,
    course_listing_id text,
    item_id text
)
AS $$
SELECT DISTINCT
    courses.course_number,
    iext.barcode AS item_barcode,
    iext.effective_call_number AS call_number,
    inst.title AS instance_title,
    COALESCE(lit.circ_count, 0) AS circ_count,
    reserves.__current AS is_current,
    courses.course_listing_id,
    reserves.item_id
FROM 
    folio_courses.coursereserves_courses__t__ courses
INNER JOIN folio_courses.coursereserves_reserves__t__ reserves
       ON courses.course_listing_id = reserves.course_listing_id
-- Join to courselistings to get term_id for filtering active courses
LEFT JOIN folio_courses.coursereserves_courselistings__t__ courselistings
       ON courses.course_listing_id = courselistings.id
-- Join to terms table for term-based date filtering and active course filtering
LEFT JOIN folio_courses.coursereserves_terms__t__ terms
       ON courselistings.term_id = terms.id
LEFT JOIN folio_derived.item_ext iext
       ON reserves.item_id = iext.item_id
LEFT JOIN folio_derived.holdings_ext hrt 
       ON iext.holdings_record_id = hrt.holdings_id
LEFT JOIN folio_derived.instance_ext inst -- get human readable title
       ON hrt.instance_id = inst.instance_id
LEFT JOIN LATERAL (
        SELECT -- Count checkouts per item, using term dates if term_name provided
            item_id,
            COUNT(loan_id) AS circ_count
        FROM folio_derived.loans_items
        WHERE 
            -- Use term dates if term_name is provided and matched, otherwise use date parameters
            loan_date::date >= COALESCE(
                CASE WHEN $3 IS NOT NULL AND $3 <> '' AND terms.name = $3 
                     THEN terms.start_date 
                     ELSE NULL 
                END,
                $1
            )
            AND loan_date::date <= COALESCE(
                CASE WHEN $3 IS NOT NULL AND $3 <> '' AND terms.name = $3 
                     THEN terms.end_date 
                     ELSE NULL 
                END,
                $2
            )
            AND item_id = reserves.item_id
        GROUP BY item_id
) lit ON true
WHERE 
    reserves.item_id IS NOT NULL
    -- Filter by __current unless show_historical_data = '1'
    -- When show_historical_data = '1', show all reserves (current and historical)
    AND (
        $6 = '1' OR reserves.__current = true
    )
    -- Filter by course codes if provided
    AND (
        $4 IS NULL 
        OR $4 = ''
        OR courses.course_number = ANY(
            string_to_array(
                regexp_replace(
                    regexp_replace(trim($4), '([A-Z])(\d)', '\1 \2', 'g'),
                    '\s*,\s*',
                    ',',
                    'g'
                ),
                ','
            )
        )
    )
    -- Filter by exclusions if provided
    AND (
        $5 IS NULL OR (
            ($5 NOT ILIKE '%POP%' OR courses.course_number IS DISTINCT FROM 'POP') AND
            ($5 NOT ILIKE '%LAW%' OR (courses.course_number IS NULL OR courses.course_number = '' OR courses.course_number NOT ILIKE 'LAW%')) AND 
            ($5 NOT ILIKE '%NEW%' OR courses.course_number IS DISTINCT FROM 'NEW') AND
            ($5 NOT ILIKE '%EMPTY%' OR (courses.course_number IS NOT NULL AND courses.course_number <> ''))
        )
    )
    -- Filter by active courses unless show_historical_data = '1'
    -- A course is active if current date is between the term's start and end dates
    -- When show_historical_data = '1', show all courses (active and historical)
    AND (
        $6 = '1' 
        OR terms.id IS NULL 
        OR (CURRENT_DATE >= terms.start_date AND CURRENT_DATE <= terms.end_date)
    )
ORDER BY 
    courses.course_number, inst.title
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;