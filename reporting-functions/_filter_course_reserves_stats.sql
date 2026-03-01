--metadb:function _filter_course_reserves_stats

DROP FUNCTION IF EXISTS _filter_course_reserves_stats;

CREATE FUNCTION _filter_course_reserves_stats(
    start_date date DEFAULT '0001-01-01',
    end_date   date DEFAULT '9999-12-31',
    term_name text DEFAULT NULL,
    course_codes text DEFAULT NULL,
    exclusions text DEFAULT NULL,
    show_historical_reserves text DEFAULT NULL,
    show_historical_checkouts text DEFAULT NULL
)
RETURNS TABLE(
    course_term text,
    course_number text,
    item_barcode text,
    call_number text,
    instance_title text,
    checkout_count bigint,
    is_current boolean,
    course_listing_id text,
    item_id text,
    reserves_start_date date
)
AS $$
SELECT
    (
        SELECT t_match.name
        FROM folio_courses.coursereserves_terms__t__ t_match
        WHERE t_match.start_date::date = reserves.start_date::date
        ORDER BY t_match.start_date DESC
        LIMIT 1
    ) AS course_term,
    courses.course_number,
    iext.barcode AS item_barcode,
    iext.effective_call_number AS call_number,
    inst.title AS instance_title,
    COUNT(li.__id) AS checkout_count,
    reserves.__current AS is_current,
    courses.course_listing_id,
    reserves.item_id,
    reserves.start_date AS reserves_start_date
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
LEFT JOIN folio_circulation.loan__t__ li
       ON iext.item_id = li.item_id
       AND (
           $7 = '1' OR (
               li.__start::date >= COALESCE(
                   (SELECT t2.start_date FROM folio_courses.coursereserves_terms__t__ t2 WHERE t2.name = $3 LIMIT 1),
                   (SELECT t3.start_date FROM folio_courses.coursereserves_terms__t__ t3 WHERE CURRENT_DATE >= t3.start_date AND CURRENT_DATE <= t3.end_date LIMIT 1),
                   terms.start_date,
                   $1
               )
               AND li.__start::date <= COALESCE(
                   (SELECT t2.end_date FROM folio_courses.coursereserves_terms__t__ t2 WHERE t2.name = $3 LIMIT 1),
                   (SELECT t3.end_date FROM folio_courses.coursereserves_terms__t__ t3 WHERE CURRENT_DATE >= t3.start_date AND CURRENT_DATE <= t3.end_date LIMIT 1),
                   terms.end_date,
                   $2
               )
           )
       ) AND (
            li.action='checkedout' OR li.action='renewed'
       )
WHERE 
    reserves.item_id IS NOT NULL
    -- Filter by __current unless show_historical_reserves = '1'
    -- When show_historical_reserves = '1', show all reserves (current and historical)
    AND (
        $6 = '1' OR reserves.__current = true
    )
    -- Filter by course codes if provided
    AND (
        $4 IS NULL 
        OR $4 = ''
        OR upper(courses.course_number) = ANY(
            string_to_array(
                regexp_replace(
                    regexp_replace(upper(trim($4)), '([A-Z])(\d)', '\1 \2', 'g'),
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
    -- Filter by active courses unless show_historical_reserves = '1'
    -- A course is active if current date is between the term's start and end dates
    -- When show_historical_reserves = '1', show all courses (active and historical)
    AND (
        $6 = '1' 
        OR terms.id IS NULL 
        OR (CURRENT_DATE >= terms.start_date AND CURRENT_DATE <= terms.end_date)
    )
GROUP BY
    courses.course_listing_id,
    courses.course_number,
    reserves.item_id,
    reserves.start_date,
    iext.barcode,
    iext.effective_call_number,
    inst.title,
    reserves.__current,
    (
        SELECT t_match.name
        FROM folio_courses.coursereserves_terms__t__ t_match
        WHERE t_match.start_date::date = reserves.start_date::date
        ORDER BY t_match.start_date DESC
        LIMIT 1
    ),
    COALESCE(
        (SELECT t2.start_date FROM folio_courses.coursereserves_terms__t__ t2 WHERE t2.name = $3 LIMIT 1),
        (SELECT t3.start_date FROM folio_courses.coursereserves_terms__t__ t3 WHERE CURRENT_DATE >= t3.start_date AND CURRENT_DATE <= t3.end_date LIMIT 1),
        terms.start_date,
        $1
    ),
    COALESCE(
        (SELECT t2.end_date FROM folio_courses.coursereserves_terms__t__ t2 WHERE t2.name = $3 LIMIT 1),
        (SELECT t3.end_date FROM folio_courses.coursereserves_terms__t__ t3 WHERE CURRENT_DATE >= t3.start_date AND CURRENT_DATE <= t3.end_date LIMIT 1),
        terms.end_date,
        $2
    )
ORDER BY 
    courses.course_number, inst.title
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;