-- get activity counts per day for one subject, with 0 count if no activity
-- present
SELECT
          timeline
        , activity_level_id
        , COUNT(activity_sleep) AS activity_minutes
FROM
          (
                    SELECT DISTINCT
                              (date_trunc('day',start_time)) AS timeline
                            , a.id                           AS activity_level_id
                    FROM
                              subject_device_activity_level
                            , activity_level a
                    WHERE
                              subject_device_id = :subjectDeviceId
                              AND a.id          < 6
          )
          AS t
          LEFT JOIN
                    subject_device_activity_level
          ON
                    date_trunc('day',start_time)=timeline
                    AND activity_level_id       =activity_sleep
                    AND subject_device_id       = :subjectDeviceId
GROUP BY
          timeline
        , activity_level_id
ORDER BY
          timeline
;

-- get activity counts per day for one subject, with 0 count if no activity
-- present
SELECT
          timeline
        , CAST(COUNT(activity_sleep) AS INTEGER) AS activity_minutes
FROM
          (
                    SELECT DISTINCT
                              (date_trunc('day',start_time)) AS timeline
                            , a.id                           AS activity_level_id
                    FROM
                              subject_device_activity_level
                            , activity_level a
                    WHERE
                              subject_device_id = :subjectDeviceId
                              AND a.id          = :activityLevelId
          )
          AS t
          LEFT JOIN
                    subject_device_activity_level
          ON
                    date_trunc('day',start_time)=timeline
                    AND activity_level_id       =activity_sleep
                    AND subject_device_id       = :subjectDeviceId
GROUP BY
          timeline
        , activity_level_id
ORDER BY
          timeline
;

-- get average activity counts per day when multiple subjects present, with 0
-- count if no activity present
SELECT
          timeline
        , activity_level_id
        , ROUND(activity_minutes/CAST(subjects AS NUMERIC),1) AS activity_minutes
FROM
          (
                    SELECT
                              timeline
                            , activity_level_id
                            , COUNT(activity_sleep) AS activity_minutes
                            , subjects
                    FROM
                              (
                                        SELECT DISTINCT
                                                  (date_trunc('day',start_time))    AS timeline
                                                , a.id                              AS activity_level_id
                                                , COUNT(DISTINCT subject_device_id) AS subjects
                                        FROM
                                                  subject_device_activity_level
                                                , activity_level a
                                        WHERE
                                                  subject_device_id IN (:subjectDeviceIds)
                                                  AND a.id < 6
                                        GROUP BY
                                                  timeline
                                                , a.id
                              )
                              AS r1
                              LEFT JOIN
                                        subject_device_activity_level
                              ON
                                        date_trunc('day',start_time) = timeline
                                        AND activity_level_id        = activity_sleep
                                        AND subject_device_id IN (:subjectDeviceIds)
                              GROUP BY
                                        timeline
                                      , activity_level_id
                                      , subjects
          )
          r2
ORDER BY
          timeline
;

-- For weekdays (SUN - THU), get average activity counts per hour with one or
-- more subjects present, and 0 count if no activity present
SELECT
          hours
        , activity_level_id
        , ROUND(COUNT(activity_sleep)/CAST(subjects AS NUMERIC),1) AS activity_minutes
FROM
          (
                    SELECT
                              extract(hours FROM timeline) AS hours
                            , COUNT(subject_device_id)        subjects
                            , activity_level_id
                    FROM
                              (
                                        SELECT DISTINCT
                                                  date_trunc('hour',start_time) AS timeline
                                                , subject_device_id
                                                , a.id AS activity_level_id
                                        FROM
                                                  subject_device_activity_level
                                                , activity_level a
                                        WHERE
                                                  subject_device_id IN (:subjectDeviceIds)
                                                  AND extract(dow FROM start_time) < 5
                                                  AND a.id                         < 6
                              )
                              r1
                    GROUP BY
                              extract(hours FROM timeline)
                            , activity_level_id
          )
          r2
          LEFT JOIN
                    subject_device_activity_level
          ON
                    extract(hours FROM start_time) = hours
                    AND activity_level_id          = activity_sleep
                    AND subject_device_id IN (:subjectDeviceIds)
                    AND extract(dow FROM start_time) < 5
GROUP BY
          hours
        , activity_level_id
        , subjects
ORDER BY
          hours
;

-- For weekend (FRI and SAT), get average activity counts per hour with one or
-- more subjects present, and 0 count if no activity present
SELECT
          hours
        , activity_level_id
        , ROUND(COUNT(activity_sleep)/CAST(subjects AS NUMERIC),1) AS activity_minutes
FROM
          (
                    SELECT
                              extract(hours FROM timeline) AS hours
                            , COUNT(subject_device_id)        subjects
                            , activity_level_id
                    FROM
                              (
                                        SELECT DISTINCT
                                                  date_trunc('hour',start_time) AS timeline
                                                , subject_device_id
                                                , a.id AS activity_level_id
                                        FROM
                                                  subject_device_activity_level
                                                , activity_level a
                                        WHERE
                                                  subject_device_id IN (:subjectDeviceIds)
                                                  AND extract(dow FROM start_time) >= 5
                                                  AND a.id                          < 6
                              )
                              r1
                    GROUP BY
                              extract(hours FROM timeline)
                            , activity_level_id
          )
          r2
          LEFT JOIN
                    subject_device_activity_level
          ON
                    extract(hours FROM start_time) = hours
                    AND activity_level_id          = activity_sleep
                    AND subject_device_id IN (:subjectDeviceIds)
                    AND extract(dow FROM start_time) >= 5
GROUP BY
          hours
        , activity_level_id
        , subjects
ORDER BY
          hours
;

-- Average activity level per day for individuals
SELECT
          activity_level_id
        , AVG(activity_minutes) AS avg_activity
FROM
          (
                    SELECT
                              timeline
                            , activity_level_id
                            , COUNT(activity_level) AS activity_minutes
                    FROM
                              (
                                        SELECT DISTINCT
                                                  (date_trunc('day',start_time)) AS timeline
                                                , a.id                           AS activity_level_id
                                        FROM
                                                  subject_device_activity_level s
                                                , activity_level                a
                                        WHERE
                                                  subject_device_id = :subjectDeviceId
                                                  AND a.id          = :activityLevelId
                              )
                              AS t
                              LEFT JOIN
                                        subject_device_activity_level
                              ON
                                        date_trunc('day',start_time)=timeline
                                        AND activity_level_id       =activity_level
                                        AND subject_device_id       = :subjectDeviceId
                    GROUP BY
                              timeline
                            , activity_level_id
          )
          AS avgperday
GROUP BY
          activity_level_i
;

SELECT
          timeline
        , activity_level_id
        , ROUND(activity_minutes/CAST(subjects AS NUMERIC),1) AS activity_minutes
FROM
          (
                    SELECT
                              timeline
                            , activity_level_id
                            , COUNT(activity_sleep) AS activity_minutes
                            , subjects
                    FROM
                              (
                                        SELECT DISTINCT
                                                  (date_trunc('day',start_time))    AS timeline
                                                , a.id                              AS activity_level_id
                                                , COUNT(DISTINCT subject_device_id) AS subjects
                                        FROM
                                                  subject_device_activity_level
                                                , activity_level a
                                        WHERE
                                                  subject_device_id IN (:subjectDeviceIds)
                                                  AND a.id < 6
                                        GROUP BY
                                                  timeline
                                                , a.id
                              )
                              AS r1
                              LEFT JOIN
                                        subject_device_activity_level
                              ON
                                        date_trunc('day',start_time) = timeline
                                        AND activity_level_id        = activity_sleep
                                        AND subject_device_id IN (:subjectDeviceIds)
                              GROUP BY
                                        timeline
                                      , activity_level_id
                                      , subjects
          )
          r2
ORDER BY
          timeline
;
