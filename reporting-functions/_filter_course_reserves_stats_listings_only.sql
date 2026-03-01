--metadb:function _filter_course_reserves_stats_listings_only

DROP FUNCTION IF EXISTS _filter_course_reserves_stats_listings_only;

CREATE FUNCTION _filter_course_reserves_stats_listings_only(
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
    is_current integer,
    course_listing_id text,
    item_id text,
    reserves_start_date date
)
AS $$
SELECT
    term_resolved.name AS course_term,
    courses.course_number,
    iext.barcode AS item_barcode,
    iext.effective_call_number AS call_number,
    inst.title AS instance_title,
    COUNT(li.__id) AS checkout_count,
    CASE WHEN reserves.__current THEN 1 ELSE 0 END AS is_current,
    courses.course_listing_id,
    reserves.item_id,
    reserves.start_date AS reserves_start_date
FROM
    folio_courses.coursereserves_courses__t__ courses
INNER JOIN folio_courses.coursereserves_reserves__t__ reserves
       ON courses.course_listing_id = reserves.course_listing_id
LEFT JOIN LATERAL (
        SELECT t.name
        FROM folio_courses.coursereserves_courses__t__ c_same
        INNER JOIN folio_courses.coursereserves_courselistings__t__ l_same
                        ON c_same.course_listing_id = l_same.id
        INNER JOIN folio_courses.coursereserves_terms__t__ t
                        ON l_same.term_id = t.id
        WHERE c_same.course_number = courses.course_number
            AND (
                    l_same.id = courses.course_listing_id
                    OR c_same.course_listing_id <> courses.course_listing_id
            )
            AND (
                coalesce(trim($4), '') <> ''
                OR $3 IS NULL
                    OR $3 = ''
                    OR t.name = $3
            )
        ORDER BY
            CASE WHEN l_same.id = courses.course_listing_id THEN 0 ELSE 1 END,
            t.start_date DESC
        LIMIT 1
) term_resolved ON true
LEFT JOIN folio_derived.item_ext iext
       ON reserves.item_id = iext.item_id
LEFT JOIN folio_derived.holdings_ext hrt
       ON iext.holdings_record_id = hrt.holdings_id
LEFT JOIN folio_derived.instance_ext inst
       ON hrt.instance_id = inst.instance_id
LEFT JOIN folio_circulation.loan__t__ li
       ON iext.item_id = li.item_id
      AND (li.action = 'checkedout' OR li.action = 'renewed')
      AND (
          coalesce(trim($4), '') <> ''
          OR lower(coalesce(trim($7), '')) IN ('1', 'true', 't', 'yes', 'y', 'on')
          OR reserves.__current = true
      )
WHERE
    reserves.item_id IS NOT NULL
    AND (
        coalesce(trim($4), '') <> ''
        OR $3 IS NULL
        OR $3 = ''
        OR term_resolved.name IS NOT NULL
    )
    AND (
        coalesce(trim($4), '') <> ''
        OR lower(coalesce(trim($6), '')) IN ('1', 'true', 't', 'yes', 'y', 'on')
        OR reserves.__current = true
    )
    AND (
        $4 IS NULL
        OR $4 = ''
        OR regexp_replace(upper(trim(courses.course_number)), '([A-Z])(\d)', '\1 \2', 'g') = ANY(
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
    AND (
        $5 IS NULL OR (
            ($5 NOT ILIKE '%POP%' OR courses.course_number IS DISTINCT FROM 'POP') AND
            ($5 NOT ILIKE '%LAW%' OR (courses.course_number IS NULL OR courses.course_number = '' OR courses.course_number NOT ILIKE 'LAW%')) AND
            ($5 NOT ILIKE '%NEW%' OR courses.course_number IS DISTINCT FROM 'NEW') AND
            ($5 NOT ILIKE '%EMPTY%' OR (courses.course_number IS NOT NULL AND courses.course_number <> ''))
        )
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
    term_resolved.name
ORDER BY
    courses.course_number, inst.title
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;
