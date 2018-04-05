--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.12
-- Dumped by pg_dump version 9.5.12

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: update_bmi(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.update_bmi() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
	BEGIN
		new.bmi = round((new.weight/(new.height/100)^2));
		RETURN new;
	END;
	$$;


ALTER FUNCTION public.update_bmi() OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: activity_level; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.activity_level (
    id smallint NOT NULL,
    name character varying(255) DEFAULT NULL::character varying,
    description character varying(255) DEFAULT NULL::character varying
);


ALTER TABLE public.activity_level OWNER TO postgres;

--
-- Name: activity_level_threshold; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.activity_level_threshold (
    id bigint NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    age integer,
    light integer,
    moderate integer,
    sedentary integer
);


ALTER TABLE public.activity_level_threshold OWNER TO postgres;

--
-- Name: activity_level_threshold_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.activity_level_threshold_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.activity_level_threshold_id_seq OWNER TO postgres;

--
-- Name: activity_level_threshold_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.activity_level_threshold_id_seq OWNED BY public.activity_level_threshold.id;

--
-- Name: raw_actigraph; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.raw_actigraph (
    id bigint NOT NULL,
    subject_device_id bigint,
    start_time timestamp without time zone NOT NULL,
    axis1 integer,
    axis2 integer,
    axis3 integer,
    steps integer,
    lux integer
);


ALTER TABLE public.raw_actigraph OWNER TO postgres;

--
-- Name: activity_minute_data; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.activity_minute_data AS
 SELECT raw_actigraph.subject_device_id,
    date_trunc('minute'::text, raw_actigraph.start_time) AS start_time,
    sum(raw_actigraph.axis1) AS axis1,
    sum(raw_actigraph.axis2) AS axis2,
    sum(raw_actigraph.axis3) AS axis3,
    sum(raw_actigraph.steps) AS steps,
    (avg(raw_actigraph.lux))::integer AS lux
   FROM public.raw_actigraph
  GROUP BY raw_actigraph.subject_device_id, (date_trunc('minute'::text, raw_actigraph.start_time));


ALTER TABLE public.activity_minute_data OWNER TO postgres;


--
-- Name: bodymetric; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bodymetric (
    id bigint NOT NULL,
    subject_id bigint NOT NULL,
    record_date date NOT NULL,
    height double precision,
    weight double precision,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    bmi integer
);


ALTER TABLE public.bodymetric OWNER TO postgres;

--
-- Name: bodymetric_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.bodymetric_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.bodymetric_id_seq OWNER TO postgres;

--
-- Name: bodymetric_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.bodymetric_id_seq OWNED BY public.bodymetric.id;


--
-- Name: cutpoint_interval; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cutpoint_interval (
    id bigint NOT NULL,
    interval_code integer NOT NULL,
    subject_device_id bigint NOT NULL,
    sleep_segment_id bigint NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    activity_level smallint
);


ALTER TABLE public.cutpoint_interval OWNER TO postgres;

--
-- Name: cutpoint_interval_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.cutpoint_interval_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.cutpoint_interval_id_seq OWNER TO postgres;

--
-- Name: cutpoint_interval_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.cutpoint_interval_id_seq OWNED BY public.cutpoint_interval.id;


--
-- Name: data_config; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.data_config (
    id bigint NOT NULL,
    subject_device_id bigint NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    data_mode_id bigint,
    epoch time without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);


ALTER TABLE public.data_config OWNER TO postgres;

--
-- Name: data_config_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.data_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.data_config_id_seq OWNER TO postgres;

--
-- Name: data_config_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.data_config_id_seq OWNED BY public.data_config.id;


--
-- Name: raw_actigraph_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.raw_actigraph_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.raw_actigraph_id_seq OWNER TO postgres;

--
-- Name: raw_actigraph_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.raw_actigraph_id_seq OWNED BY public.raw_actigraph.id;

--
-- Name: role_type; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.role_type (
    id smallint NOT NULL,
    name character varying(50),
    description character varying(255)
);


ALTER TABLE public.role_type OWNER TO postgres;

--
-- Name: sleep_segment; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.sleep_segment (
    id bigint NOT NULL,
    segment_code integer NOT NULL,
    subject_device_id bigint NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    sleep_start_time timestamp without time zone,
    ignore_segment boolean DEFAULT false
);


ALTER TABLE public.sleep_segment OWNER TO postgres;

--
-- Name: subject; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.subject (
    id bigint NOT NULL,
    birth_year smallint,
    gender character varying(10),
    provider character varying(255),
    timezone character varying(255),
    user_name character varying(255) NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);


ALTER TABLE public.subject OWNER TO postgres;

--
-- Name: subject_device; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.subject_device (
    id bigint NOT NULL,
    device_identifier character varying(255),
    subject_id bigint NOT NULL,
    device_type_id integer,
    auth_token character varying(255) DEFAULT NULL::character varying,
    device_type character varying(255),
    mac_address character varying(255),
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);


ALTER TABLE public.subject_device OWNER TO postgres;

--
-- Name: sleep_analysis; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.sleep_analysis (
    id bigint NOT NULL,
    sleep_segment_id bigint NOT NULL,
    sleep_efficiency double precision NOT NULL,
    total_sleep_time integer NOT NULL,
    total_minutes_in_bed integer NOT NULL,
    latency integer NOT NULL,
    wakefulness integer NOT NULL,
    sleep_duration integer NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);


ALTER TABLE public.sleep_analysis OWNER TO postgres;

--
-- Name: sleep_analysis_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.sleep_analysis_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sleep_analysis_id_seq OWNER TO postgres;

--
-- Name: sleep_analysis_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.sleep_analysis_id_seq OWNED BY public.sleep_analysis.id;


--
-- Name: sleep_annotation; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.sleep_annotation (
    id bigint NOT NULL,
    subject_device_id bigint NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    sleep boolean
);


ALTER TABLE public.sleep_annotation OWNER TO postgres;

--
-- Name: sleep_annotation_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.sleep_annotation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sleep_annotation_id_seq OWNER TO postgres;

--
-- Name: sleep_annotation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.sleep_annotation_id_seq OWNED BY public.sleep_annotation.id;


--
-- Name: sleep_segment_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.sleep_segment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sleep_segment_id_seq OWNER TO postgres;

--
-- Name: sleep_segment_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.sleep_segment_id_seq OWNED BY public.sleep_segment.id;


--
-- Name: subject_device_activity_level; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.subject_device_activity_level (
    id bigint NOT NULL,
    subject_device_id bigint NOT NULL,
    start_time timestamp without time zone NOT NULL,
    activity_level smallint,
    axis1 integer,
    axis2 integer,
    axis3 integer,
    steps integer,
    lux integer
);


ALTER TABLE public.subject_device_activity_level OWNER TO postgres;

--
-- Name: subject_device_activity_level_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.subject_device_activity_level_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.subject_device_activity_level_id_seq OWNER TO postgres;

--
-- Name: subject_device_activity_level_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.subject_device_activity_level_id_seq OWNED BY public.subject_device_activity_level.id;


--
-- Name: subject_device_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.subject_device_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.subject_device_id_seq OWNER TO postgres;

--
-- Name: subject_device_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.subject_device_id_seq OWNED BY public.subject_device.id;


--
-- Name: subject_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.subject_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.subject_id_seq OWNER TO postgres;


--
-- Name: subject_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.subject_id_seq OWNED BY public.subject.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.activity_level_threshold ALTER COLUMN id SET DEFAULT nextval('public.activity_level_threshold_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bodymetric ALTER COLUMN id SET DEFAULT nextval('public.bodymetric_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cutpoint_interval ALTER COLUMN id SET DEFAULT nextval('public.cutpoint_interval_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_config ALTER COLUMN id SET DEFAULT nextval('public.data_config_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.raw_actigraph ALTER COLUMN id SET DEFAULT nextval('public.raw_actigraph_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_analysis ALTER COLUMN id SET DEFAULT nextval('public.sleep_analysis_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_annotation ALTER COLUMN id SET DEFAULT nextval('public.sleep_annotation_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_segment ALTER COLUMN id SET DEFAULT nextval('public.sleep_segment_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject ALTER COLUMN id SET DEFAULT nextval('public.subject_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device ALTER COLUMN id SET DEFAULT nextval('public.subject_device_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device_activity_level ALTER COLUMN id SET DEFAULT nextval('public.subject_device_activity_level_id_seq'::regclass);


--
-- Name: actigraph_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.raw_actigraph
    ADD CONSTRAINT actigraph_pkey PRIMARY KEY (id);


--
-- Name: activity_level_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.activity_level
    ADD CONSTRAINT activity_level_pkey PRIMARY KEY (id);


--
-- Name: activity_level_threshold_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.activity_level_threshold
    ADD CONSTRAINT activity_level_threshold_pkey PRIMARY KEY (id);


--
-- Name: annotation_subject_device_start_time_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_annotation
    ADD CONSTRAINT annotation_subject_device_start_time_unique UNIQUE (subject_device_id, start_time);


--
-- Name: bodymetric_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bodymetric
    ADD CONSTRAINT bodymetric_pkey PRIMARY KEY (id);

--
-- Name: cutpoint_interval_interval_code_subject_device_id_segment_i_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cutpoint_interval
    ADD CONSTRAINT cutpoint_interval_interval_code_subject_device_id_segment_i_key UNIQUE (interval_code, subject_device_id, sleep_segment_id);


--
-- Name: cutpoint_interval_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cutpoint_interval
    ADD CONSTRAINT cutpoint_interval_pkey PRIMARY KEY (id);


--
-- Name: data_config_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_config
    ADD CONSTRAINT data_config_pkey PRIMARY KEY (id);


--
-- Name: device_subject_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device
    ADD CONSTRAINT device_subject_unique_key UNIQUE (device_identifier, subject_id);


--
-- Name: sleep_analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_analysis
    ADD CONSTRAINT sleep_analysis_pkey PRIMARY KEY (id);


--
-- Name: sleep_annotation_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_annotation
    ADD CONSTRAINT sleep_annotation_pkey PRIMARY KEY (id);


--
-- Name: sleep_segment_analysis_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_analysis
    ADD CONSTRAINT sleep_segment_analysis_unique UNIQUE (sleep_segment_id);


--
-- Name: sleep_segment_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_segment
    ADD CONSTRAINT sleep_segment_pkey PRIMARY KEY (id);
--
-- Name: subject_device_activity_level_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device_activity_level
    ADD CONSTRAINT subject_device_activity_level_pkey PRIMARY KEY (id);


--
-- Name: subject_device_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device
    ADD CONSTRAINT subject_device_pkey PRIMARY KEY (id);


--
-- Name: subject_device_sleep_segment_interval_code_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cutpoint_interval
    ADD CONSTRAINT subject_device_sleep_segment_interval_code_unique_key UNIQUE (subject_device_id, sleep_segment_id, interval_code);


--
-- Name: subject_device_start_time_segment_code_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_segment
    ADD CONSTRAINT subject_device_start_time_segment_code_unique_key UNIQUE (subject_device_id, start_time, segment_code);


--
-- Name: subject_device_start_time_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_config
    ADD CONSTRAINT subject_device_start_time_unique_key UNIQUE (subject_device_id, start_time);


--
-- Name: subject_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject
    ADD CONSTRAINT subject_pkey PRIMARY KEY (id);


--
-- Name: subject_record_date_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bodymetric
    ADD CONSTRAINT subject_record_date_unique_key UNIQUE (subject_id, record_date);


--
-- Name: uk_3uqvm478qj5e66b5uitwfsdn; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_config
    ADD CONSTRAINT uk_3uqvm478qj5e66b5uitwfsdn UNIQUE (subject_device_id, start_time);


--
-- Name: uk_bhq9a0ewghxt7jxrsogg0ijku; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cutpoint_interval
    ADD CONSTRAINT uk_bhq9a0ewghxt7jxrsogg0ijku UNIQUE (subject_device_id, sleep_segment_id, interval_code);


--
-- Name: uk_gokh197649drlswkc3qnvtf0d; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bodymetric
    ADD CONSTRAINT uk_gokh197649drlswkc3qnvtf0d UNIQUE (subject_id, record_date);


--
-- Name: unique_bodymetric_subject; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bodymetric
    ADD CONSTRAINT unique_bodymetric_subject UNIQUE (subject_id, record_date);


--
-- Name: unique_device_starttime; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_config
    ADD CONSTRAINT unique_device_starttime UNIQUE (subject_device_id, start_time);--
-- Name: unique_user_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject
    ADD CONSTRAINT unique_user_name UNIQUE (user_name);


--
-- Name: actigraph_subject_device_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX actigraph_subject_device_id_idx ON public.raw_actigraph USING btree (subject_device_id);


--
-- Name: activity_level_day_grp; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_day_grp ON public.subject_device_activity_level USING btree (date_trunc('day'::text, start_time), activity_level);


--
-- Name: activity_level_extract_hour; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_extract_hour ON public.subject_device_activity_level USING btree (activity_level, date_part('hours'::text, start_time));


--
-- Name: activity_level_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_id ON public.activity_level USING btree (id);


--
-- Name: activity_level_sd; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_sd ON public.subject_device_activity_level USING btree (activity_level);


--
-- Name: activity_level_sd_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_sd_id ON public.subject_device_activity_level USING btree (subject_device_id);


--
-- Name: activity_level_time; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_time ON public.subject_device_activity_level USING btree (start_time);


--
-- Name: activity_level_time_dow; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_time_dow ON public.subject_device_activity_level USING btree (date_part('dow'::text, start_time));


--
-- Name: activity_level_time_hour; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX activity_level_time_hour ON public.subject_device_activity_level USING btree (date_part('hours'::text, start_time));


--
-- Name: bodymetric_subject_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bodymetric_subject_id_idx ON public.bodymetric USING btree (subject_id);


--
-- Name: cutpoint_interval_end_time_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX cutpoint_interval_end_time_idx ON public.cutpoint_interval USING btree (end_time);


--
-- Name: cutpoint_interval_segment_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX cutpoint_interval_segment_id_idx ON public.cutpoint_interval USING btree (sleep_segment_id);


--
-- Name: cutpoint_interval_start_time_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX cutpoint_interval_start_time_idx ON public.cutpoint_interval USING btree (start_time);


--
-- Name: cutpoint_interval_subject_device_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX cutpoint_interval_subject_device_id_idx ON public.cutpoint_interval USING btree (subject_device_id);


--
-- Name: data_config_subject_device_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX data_config_subject_device_id_idx ON public.data_config USING btree (subject_device_id);


--
-- Name: segment_per_day; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX segment_per_day ON public.sleep_segment USING btree (subject_device_id, date_trunc('day'::text, start_time));


--
-- Name: sleep_analysis_segment_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX sleep_analysis_segment_id_idx ON public.sleep_analysis USING btree (sleep_segment_id);


--
-- Name: sleep_annotation_start_time_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX sleep_annotation_start_time_idx ON public.sleep_annotation USING btree (start_time);


--
-- Name: sleep_annotation_subject_device_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX sleep_annotation_subject_device_id_idx ON public.sleep_annotation USING btree (subject_device_id);


--
-- Name: sleep_segment_end_time_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX sleep_segment_end_time_idx ON public.sleep_segment USING btree (end_time);


--
-- Name: sleep_segment_start_time_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX sleep_segment_start_time_idx ON public.sleep_segment USING btree (start_time);


--
-- Name: sleep_segment_subject_device_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX sleep_segment_subject_device_id_idx ON public.sleep_segment USING btree (subject_device_id);


--
-- Name: subject_device_device_type_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX subject_device_device_type_id_idx ON public.subject_device USING btree (device_type_id);


--
-- Name: subject_device_subject_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX subject_device_subject_id_idx ON public.subject_device USING btree (subject_id);



--
-- Name: trigger_update_bmi; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trigger_update_bmi BEFORE INSERT ON public.bodymetric FOR EACH ROW EXECUTE PROCEDURE public.update_bmi();


--
-- Name: bodymetric_subject_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bodymetric
    ADD CONSTRAINT bodymetric_subject_fk FOREIGN KEY (subject_id) REFERENCES public.subject(id) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: device_type_device_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device
    ADD CONSTRAINT device_type_device_fk FOREIGN KEY (device_type_id) REFERENCES public.device_type(id) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_actigraph_device; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.raw_actigraph
    ADD CONSTRAINT fk_actigraph_device FOREIGN KEY (subject_device_id) REFERENCES public.subject_device(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_activity_level_device; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device_activity_level
    ADD CONSTRAINT fk_activity_level_device FOREIGN KEY (subject_device_id) REFERENCES public.subject_device(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_cpinterval_device; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cutpoint_interval
    ADD CONSTRAINT fk_cpinterval_device FOREIGN KEY (subject_device_id) REFERENCES public.subject_device(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_cpinterval_segment; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cutpoint_interval
    ADD CONSTRAINT fk_cpinterval_segment FOREIGN KEY (sleep_segment_id) REFERENCES public.sleep_segment(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_data_config_device; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_config
    ADD CONSTRAINT fk_data_config_device FOREIGN KEY (subject_device_id) REFERENCES public.subject_device(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_sleep_analysis_segment; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_analysis
    ADD CONSTRAINT fk_sleep_analysis_segment FOREIGN KEY (sleep_segment_id) REFERENCES public.sleep_segment(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_sleep_ann_device; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_annotation
    ADD CONSTRAINT fk_sleep_ann_device FOREIGN KEY (subject_device_id) REFERENCES public.subject_device(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fk_sleep_device; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sleep_segment
    ADD CONSTRAINT fk_sleep_device FOREIGN KEY (subject_device_id) REFERENCES public.subject_device(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: subject_device_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.subject_device
    ADD CONSTRAINT subject_device_fk FOREIGN KEY (subject_id) REFERENCES public.subject(id) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

