--metadb:function _debug_course_reserves_by_course_number

DROP FUNCTION IF EXISTS _debug_course_reserves_by_course_number;

CREATE FUNCTION _debug_course_reserves_by_course_number(
    course_code text
)
RETURNS TABLE(
    input_course_code text,
    matched_course_number text,
    course_listing_id text,
    course_id text,
    term_id text,
    term_name text,
    reserve_rows bigint,
    reserve_rows_with_item bigint,
    distinct_item_count bigint,
    current_reserve_rows bigint,
    historical_reserve_rows bigint
)
AS $$
SELECT
    regexp_replace(upper(trim(course_code)), '([A-Z])(\d)', '\1 \2', 'g') AS input_course_code,
    c.course_number AS matched_course_number,
    c.course_listing_id,
    c.id AS course_id,
    l.term_id,
    t.name AS term_name,
    COUNT(r.*)::bigint AS reserve_rows,
    COUNT(*) FILTER (WHERE r.item_id IS NOT NULL)::bigint AS reserve_rows_with_item,
    COUNT(DISTINCT r.item_id)::bigint AS distinct_item_count,
    COUNT(*) FILTER (WHERE r.__current = true)::bigint AS current_reserve_rows,
    COUNT(*) FILTER (WHERE r.__current = false)::bigint AS historical_reserve_rows
FROM folio_courses.coursereserves_courses__t__ c
LEFT JOIN folio_courses.coursereserves_courselistings__t__ l
       ON c.course_listing_id = l.id
LEFT JOIN folio_courses.coursereserves_terms__t__ t
       ON l.term_id = t.id
LEFT JOIN folio_courses.coursereserves_reserves__t__ r
       ON c.course_listing_id = r.course_listing_id
WHERE regexp_replace(upper(trim(c.course_number)), '([A-Z])(\d)', '\1 \2', 'g') =
      regexp_replace(upper(trim(course_code)), '([A-Z])(\d)', '\1 \2', 'g')
GROUP BY
    c.course_number,
    c.course_listing_id,
    c.id,
    l.term_id,
    t.name
ORDER BY
    c.course_number,
    c.course_listing_id;
$$
LANGUAGE sql
STABLE
PARALLEL SAFE;
