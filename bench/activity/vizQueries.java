package qa.qcri.rahar.repository;

import java.sql.Timestamp;
import java.util.List;

import org.hibernate.stat.internal.SessionStatisticsImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import qa.qcri.rahar.common.ActivityLevel;
import qa.qcri.rahar.entity.SubjectDeviceActivityLevel;

public interface SubjectDeviceActivityLevelRepository extends PagingAndSortingRepository<SubjectDeviceActivityLevel, Long> {

	SubjectDeviceActivityLevel findBySubjectDeviceIdAndStartTime(Long subjectDeviceId, Timestamp startTime);

	List<SubjectDeviceActivityLevel> findBySubjectDeviceIdAndStartTimeBetweenOrderByStartTime(Long subjectDeviceId, 
			Timestamp startTime, Timestamp endTime);

	List<SubjectDeviceActivityLevel> findBySubjectDeviceIdInOrderById(List<Long> subjectDeviceId);

	List<SubjectDeviceActivityLevel> findBySubjectDeviceIdInAndStartTimeBetweenOrderByStartTime(
			List<Long> subjectDeviceId, Timestamp startTime, Timestamp endTime);

	List<SubjectDeviceActivityLevel> findTop20BySubjectDeviceIdIn(List<Long> subjectDeviceId);

	List<SubjectDeviceActivityLevel> findBySubjectDeviceIdInOrderById(List<Long> deviceIds, Pageable pageRequest);
	
	List<SubjectDeviceActivityLevel> findAll();
	
	//get activity counts per day for one subject, with 0 count if no activity present
	@Query(value="select timeline, activity_level_id, count(activity_sleep) as activity_minutes from "
			+ "(select distinct(date_trunc('day',start_time)) as timeline, a.id as activity_level_id from subject_device_activity_level, "
			+ "activity_level a where subject_device_id = :subjectDeviceId and a.id < 6) as t left join subject_device_activity_level on "
			+ "date_trunc('day',start_time)=timeline and activity_level_id=activity_sleep and subject_device_id = :subjectDeviceId "
			+ "group by timeline, activity_level_id order by timeline",
			nativeQuery = true)
	List<Object[]> findActivityCountPerDay(@Param("subjectDeviceId") Long subjectDeviceId);
	
	//get activity counts per day for one subject, with 0 count if no activity present
	@Query(value="select timeline,cast(count(activity_sleep) as INTEGER) as activity_minutes from "
			+ "(select distinct(date_trunc('day',start_time)) as timeline, a.id as activity_level_id from subject_device_activity_level, "
			+ "activity_level a where subject_device_id = :subjectDeviceId and a.id = :activityLevelId) as t left join subject_device_activity_level on "
			+ "date_trunc('day',start_time)=timeline and activity_level_id=activity_sleep and subject_device_id = :subjectDeviceId "
			+ "group by timeline, activity_level_id order by timeline",
			nativeQuery = true)
	List<Object[]> findActivityCountPerDay(@Param("subjectDeviceId") Long subjectDeviceId, @Param("activityLevelId") Integer activityLevelId);
	
	//get average activity counts per day when multiple subjects present, with 0 count if no activity present
	@Query(value="select timeline, activity_level_id, round(activity_minutes/cast(subjects as numeric),1) as activity_minutes from "
			+ "(select timeline, activity_level_id, count(activity_sleep) as activity_minutes, subjects from "
			+ "(select distinct(date_trunc('day',start_time)) as timeline, a.id as activity_level_id, "
			+ "count(distinct subject_device_id) as subjects from subject_device_activity_level, activity_level a where "
			+ "subject_device_id in (:subjectDeviceIds) and a.id < 6 group by timeline, a.id) as r1 left join subject_device_activity_level "
			+ "on date_trunc('day',start_time) = timeline and activity_level_id = activity_sleep and subject_device_id in (:subjectDeviceIds) "
			+ "group by timeline, activity_level_id,subjects) r2 order by timeline",
			nativeQuery = true)
	List<Object[]> findGroupActivityCountPerDay(@Param("subjectDeviceIds") List<Long> subjectDeviceIds);


	//For weekdays (SUN - THU), get average activity counts per hour with one or more subjects present, and 0 count if no activity present
	@Query(value="select hours, activity_level_id, round(count(activity_sleep)/cast(subjects as numeric),1) as activity_minutes from "
			+ "(select extract(hours from timeline) as hours, count(subject_device_id) subjects, activity_level_id from "
			+ "(select distinct date_trunc('hour',start_time) as timeline, subject_device_id, a.id as activity_level_id from "
			+ "subject_device_activity_level, activity_level a where subject_device_id in (:subjectDeviceIds) and "
			+ "extract(dow from start_time) < 5 and a.id < 6) r1 group by extract(hours from timeline), activity_level_id) r2 "
			+ "left join subject_device_activity_level on extract(hours from start_time) = hours and activity_level_id = activity_sleep "
			+ "and subject_device_id in (:subjectDeviceIds) and extract(dow from start_time) < 5 "
			+ "group by hours, activity_level_id, subjects order by hours",
	nativeQuery = true)
	List<Object[]> findWeekdayActivityPerHour(@Param("subjectDeviceIds") List<Long> subjectDeviceIds);

	//For weekend (FRI and SAT), get average activity counts per hour with one or more subjects present, and 0 count if no activity present
	@Query(value="select hours, activity_level_id, round(count(activity_sleep)/cast(subjects as numeric),1) as activity_minutes from "
			+ "(select extract(hours from timeline) as hours, count(subject_device_id) subjects, activity_level_id from "
			+ "(select distinct date_trunc('hour',start_time) as timeline, subject_device_id, a.id as activity_level_id from "
			+ "subject_device_activity_level, activity_level a where subject_device_id in (:subjectDeviceIds) and "
			+ "extract(dow from start_time) >= 5 and a.id < 6) r1 group by extract(hours from timeline), activity_level_id) r2 "
			+ "left join subject_device_activity_level on extract(hours from start_time) = hours and activity_level_id = activity_sleep "
			+ "and subject_device_id in (:subjectDeviceIds) and extract(dow from start_time) >= 5 "
			+ "group by hours, activity_level_id, subjects order by hours",
	nativeQuery = true)
	List<Object[]> findWeekendActivityPerHour(@Param("subjectDeviceIds") List<Long> subjectDeviceIds);

	SubjectDeviceActivityLevel findFirstBySubjectDeviceIdInOrderByStartTime(List<Long> deviceIds);

	SubjectDeviceActivityLevel findTopBySubjectDeviceIdInOrderByStartTimeDesc(List<Long> deviceIds);
		
	//Average activity level per day for individuals
	@Query(value="select activity_level_id, avg(activity_minutes) as avg_activity from (select timeline, activity_level_id, count(activity_level) "
			+ "as activity_minutes from (select distinct(date_trunc('day',start_time)) as timeline, a.id as activity_level_id "
			+ "from subject_device_activity_level s,activity_level a where subject_device_id = :subjectDeviceId and a.id = :activityLevelId) as t "
			+ "left join subject_device_activity_level on date_trunc('day',start_time)=timeline and activity_level_id=activity_level and "
			+ "subject_device_id = :subjectDeviceId group by timeline, activity_level_id) as avgperday group by activity_level_id"
			,nativeQuery = true)
	List<Object[]> findAverageActivityCountPerDay(@Param("subjectDeviceId") Long subjectDeviceId, @Param("activityLevelId") Integer activityLevelId);

	@Query(value="select timeline, activity_level_id, round(activity_minutes/cast(subjects as numeric),1) as activity_minutes from "
			+ "(select timeline, activity_level_id, count(activity_sleep) as activity_minutes, subjects from "
			+ "(select distinct(date_trunc('day',start_time)) as timeline, a.id as activity_level_id, "
			+ "count(distinct subject_device_id) as subjects from subject_device_activity_level, activity_level a where "
			+ "subject_device_id in (:subjectDeviceIds) and a.id < 6 group by timeline, a.id) as r1 left join subject_device_activity_level "
			+ "on date_trunc('day',start_time) = timeline and activity_level_id = activity_sleep and subject_device_id in (:subjectDeviceIds) "
			+ "group by timeline, activity_level_id,subjects) r2 order by timeline",
			nativeQuery = true)
	List<Integer> findGroupActivityCountPerDay(List<Long> deviceIds, Integer activityLevelId);

}

