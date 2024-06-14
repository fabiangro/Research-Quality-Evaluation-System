--
-- PostgreSQL database dump
--

-- Dumped from database version 16.2 (Ubuntu 16.2-1.pgdg23.04+1)
-- Dumped by pg_dump version 16.2 (Ubuntu 16.2-1.pgdg23.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


--
-- Name: check_pwd(jsonb); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.check_pwd(args jsonb) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
	passed BOOLEAN := FALSE;
BEGIN
	SELECT (au.pwd = args->>'password') INTO passed
	FROM 
		api_user au
	WHERE
		au.login=args->>'login';
	RETURN passed;
END
$$;


-- ALTER FUNCTION public.check_pwd(args jsonb) OWNER TO postgres;

--
-- Name: del_user(jsonb); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.del_user(args jsonb) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
	passed BOOLEAN := FALSE;
BEGIN
	SELECT (pwd = args->>'password') INTO passed
	FROM 
		api_user
	WHERE
		login=args->>'login';
	IF passed THEN 
		DELETE FROM api_user WHERE login=args->>'login';
		RETURN 'OK';
	ELSE
		RETURN 'ERROR';
	END IF;
END
$$;


-- ALTER FUNCTION public.del_user(args jsonb) OWNER TO postgres;

--
-- Name: get_author_details(jsonb); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_author_details(args jsonb) RETURNS TABLE(conference character varying, year numeric, number_of_points integer, institution character varying, title character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
	SELECT DISTINCT
		ap.conference AS conference,
		EXTRACT(YEAR FROM ap.publication_date) AS year,
		ap.points,
		aa.institution AS institution,
		ap.title
	FROM 
		article_points ap
	JOIN 
		article_author aa ON ap.article_id = aa.article_id
	WHERE 
		aa.author = args->>'author'
	ORDER BY 
		ap.conference,
		ap.points DESC,
		ap.title ASC;
END;
$$;


-- ALTER FUNCTION public.get_author_details(args jsonb) OWNER TO postgres;

--
-- Name: get_author_points(jsonb); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_author_points(args jsonb) RETURNS TABLE(author character varying, points bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
	SELECT
		aa.author AS author,
		SUM(ap.points) AS points
	FROM
		article_points ap
	JOIN
		article_author aa ON ap.article_id = aa.article_id
	WHERE
		ap.publication_date BETWEEN DATE(args->>'start_date')
								AND DATE(args->>'end_date')
	GROUP BY
		aa.author
	ORDER BY
		points DESC;
END
$$;


-- ALTER FUNCTION public.get_author_points(args jsonb) OWNER TO postgres;

--
-- Name: get_institution_points(jsonb); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_institution_points(args jsonb) RETURNS TABLE(institutions character varying, points numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
	RETURN QUERY
    WITH author_counts AS (
        SELECT 
            ap.article_id,
            ap.points,
            inst.name AS institution,
            COUNT(*) FILTER (WHERE aa.institution = inst.name) AS institution_authors
        FROM 
            article_points ap
        JOIN 
            article_author aa ON ap.article_id = aa.article_id
        JOIN 
            institution inst ON aa.institution = inst.name
        WHERE
            ap.publication_date BETWEEN DATE(args->>'start_date') AND DATE(args->>'end_date')
        GROUP BY 
            ap.article_id, ap.points, inst.name
    ),
    total_authors AS (
        SELECT 
            ap.article_id,
            COUNT(*) AS total_authors
        FROM 
            article_points ap
        JOIN 
            article_author aa ON ap.article_id = aa.article_id
        WHERE
            ap.publication_date BETWEEN DATE(args->>'start_date') AND DATE(args->>'end_date')
        GROUP BY 
            ap.article_id
    ),
    author_sums AS (
        SELECT
            ac.article_id,
            ac.institution,
            ac.points,
            ac.institution_authors,
            ta.total_authors
        FROM author_counts ac
        JOIN total_authors ta ON ac.article_id = ta.article_id
    ),
    institution_points AS (
        SELECT 
            institution,
            CASE
                WHEN author_sums.points IN (200, 140, 100) THEN author_sums.points
                WHEN author_sums.points = 70 THEN GREATEST(70 * sqrt(institution_authors::NUMERIC / total_authors), 7)
                WHEN author_sums.points = 20 THEN GREATEST(20 * institution_authors::NUMERIC / total_authors, 2)
                ELSE 0
            END AS calculated_points
        FROM author_sums
    )
    SELECT 
        institution AS institutions,
        ROUND(SUM(calculated_points), 4) AS calculated_points
    FROM 
        institution_points
    GROUP BY 
        institution
    ORDER BY 
        calculated_points DESC;
END
$$;


-- ALTER FUNCTION public.get_institution_points(args jsonb) OWNER TO postgres;

--
-- Name: new_article(jsonb); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.new_article(IN args jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
	article_id INT;
	authors jsonb;
BEGIN
	INSERT INTO article (title, conference, publication_date) VALUES (args->>'title', args->>'conference', DATE(args->>'date'))  RETURNING id INTO article_id;
	
	FOR authors IN SELECT * FROM jsonb_array_elements(args->'author')
	LOOP
		INSERT INTO article_author VALUES(article_id, authors->>'author', authors->>'institution');
	END LOOP;
END
$$;


-- ALTER PROCEDURE public.new_article(IN args jsonb) OWNER TO postgres;

--
-- Name: new_article(text, date, text); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.new_article(IN title text, IN publication_date date, IN conference_name text)
    LANGUAGE plpgsql
    AS $$
DECLARE
	conference_id INT;
BEGIN
	SELECT id INTO conference_id FROM conference c WHERE c.name = conference_name;
	INSERT INTO article VALUES (title, publication_date, conference_id);
END
$$;


-- ALTER PROCEDURE public.new_article(IN title text, IN publication_date date, IN conference_name text) OWNER TO postgres;



--
-- Name: new_aut(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.new_aut() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
	INSERT INTO author ("name") VALUES (NEW.author) ON CONFLICT DO NOTHING;
	INSERT INTO institution ("name") VALUES (NEW.institution) ON CONFLICT DO NOTHING;
	RETURN NEW;
END;
$$;


-- ALTER FUNCTION public.new_aut() OWNER TO postgres;

--
-- Name: new_conf(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.new_conf() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO conference (name) VALUES (NEW.conference) ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;


-- ALTER FUNCTION public.new_conf() OWNER TO postgres;

--
-- Name: new_conference_points(jsonb); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.new_conference_points(IN args jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
	points_list jsonb;
	change_date DATE := DATE(args->>'date');
BEGIN
	FOR points_list IN SELECT * FROM jsonb_array_elements(args->'points_list')
	LOOP
		INSERT INTO conference_points (conference, start_date, points)
			VALUES (points_list->>'conference', change_date, (points_list->>'points')::INT);
	END LOOP;
END
$$;


-- ALTER PROCEDURE public.new_conference_points(IN args jsonb) OWNER TO postgres;




--
-- Name: new_user(jsonb); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.new_user(IN args jsonb)
    LANGUAGE plpgsql
    AS $$
BEGIN
	INSERT INTO api_user (login, "pwd") VALUES (args->>'newlogin', args->>'newpassword');
END
$$;


-- ALTER PROCEDURE public.new_user(IN args jsonb) OWNER TO postgres;



SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: api_user; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.api_user (
    login character varying(50) NOT NULL,
    pwd character varying(50) NOT NULL
);


-- ALTER TABLE public.api_user OWNER TO postgres;

--
-- Name: article; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.article (
    id integer NOT NULL,
    title character varying(255) NOT NULL,
    conference character varying(250),
    publication_date date NOT NULL
);


-- ALTER TABLE public.article OWNER TO postgres;

--
-- Name: article_author; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.article_author (
    article_id integer,
    author character varying(250),
    institution character varying(250)
);


-- ALTER TABLE public.article_author OWNER TO postgres;

--
-- Name: conference_points; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.conference_points (
    conference character varying(250) NOT NULL,
    start_date date NOT NULL,
    points integer NOT NULL
);


-- ALTER TABLE public.conference_points OWNER TO postgres;

--
-- Name: article_details; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.article_details AS
 SELECT a.title,
    a.publication_date,
    a.conference,
    cp.points,
    aa.author,
    aa.institution,
    EXTRACT(year FROM a.publication_date) AS year
   FROM ((public.article a
     JOIN public.conference_points cp ON (((a.conference)::text = (cp.conference)::text)))
     JOIN public.article_author aa ON ((a.id = aa.article_id)))
  WHERE (cp.start_date <= a.publication_date)
  ORDER BY cp.start_date DESC;


-- ALTER VIEW public.article_details OWNER TO postgres;

--
-- Name: article_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.article_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


-- ALTER SEQUENCE public.article_id_seq OWNER TO postgres;

--
-- Name: article_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

-- ALTER SEQUENCE public.article_id_seq OWNED BY public.article.id;


--
-- Name: article_points; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.article_points AS
 SELECT a.id AS article_id,
    a.title,
    a.publication_date,
    a.conference,
    cp.points
   FROM (public.article a
     LEFT JOIN LATERAL ( SELECT cp_1.points
           FROM public.conference_points cp_1
          WHERE (((cp_1.conference)::text = (a.conference)::text) AND (cp_1.start_date <= a.publication_date))
          ORDER BY cp_1.start_date DESC
         LIMIT 1) cp ON (true));


-- ALTER VIEW public.article_points OWNER TO postgres;

--
-- Name: author; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.author (
    name character varying(255) NOT NULL
);


-- ALTER TABLE public.author OWNER TO postgres;

--
-- Name: conference; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.conference (
    name character varying(255) NOT NULL
);


-- ALTER TABLE public.conference OWNER TO postgres;

--
-- Name: institution; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.institution (
    name character varying(255) NOT NULL
);


-- ALTER TABLE public.institution OWNER TO postgres;

--
-- Name: article id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.article ALTER COLUMN id SET DEFAULT nextval('public.article_id_seq'::regclass);

--
-- Name: article_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.article_id_seq', 38, true);


--
-- Name: api_user api_user_login_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_user
    ADD CONSTRAINT api_user_login_key UNIQUE (login);


--
-- Name: article_author article_author_author_article_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.article_author
    ADD CONSTRAINT article_author_author_article_id_key UNIQUE (author, article_id);


--
-- Name: article article_conference_title_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.article
    ADD CONSTRAINT article_conference_title_key UNIQUE (conference, title);


--
-- Name: article article_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.article
    ADD CONSTRAINT article_pkey PRIMARY KEY (id);


--
-- Name: author author_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.author
    ADD CONSTRAINT author_pkey PRIMARY KEY (name);


--
-- Name: conference conference_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.conference
    ADD CONSTRAINT conference_pkey PRIMARY KEY (name);


--
-- Name: conference_points conference_points_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.conference_points
    ADD CONSTRAINT conference_points_pkey PRIMARY KEY (conference, start_date);


--
-- Name: institution institution_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.institution
    ADD CONSTRAINT institution_pkey PRIMARY KEY (name);


--
-- Name: article_author new_author; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER new_author BEFORE INSERT ON public.article_author FOR EACH ROW EXECUTE FUNCTION public.new_aut();


--
-- Name: conference_points new_conference; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER new_conference BEFORE INSERT ON public.conference_points FOR EACH ROW EXECUTE FUNCTION public.new_conf();


--
-- Name: article new_conference_from_article; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER new_conference_from_article BEFORE INSERT ON public.article FOR EACH ROW EXECUTE FUNCTION public.new_conf();


--
-- Name: conference_points new_conference_from_points; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER new_conference_from_points BEFORE INSERT ON public.conference_points FOR EACH ROW EXECUTE FUNCTION public.new_conf();


--
-- Name: article_author article_author_article_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.article_author
    ADD CONSTRAINT article_author_article_id_fkey FOREIGN KEY (article_id) REFERENCES public.article(id);


--
-- Name: article_author article_author_author_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.article_author
    ADD CONSTRAINT article_author_author_fkey FOREIGN KEY (author) REFERENCES public.author(name);


--
-- Name: article_author article_author_institution_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.article_author
    ADD CONSTRAINT article_author_institution_fkey FOREIGN KEY (institution) REFERENCES public.institution(name);


--
-- Name: conference_points conference_points_conference_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.conference_points
    ADD CONSTRAINT conference_points_conference_fkey FOREIGN KEY (conference) REFERENCES public.conference(name);


--
-- Name: TABLE article; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.article TO api_user;


--
-- Name: TABLE article_author; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.article_author TO api_user;


--
-- Name: TABLE conference_points; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.conference_points TO api_user;


--
-- Name: TABLE article_details; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.article_details TO api_user;


--
-- Name: TABLE article_points; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.article_points TO api_user;


--
-- Name: TABLE author; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.author TO api_user;


--
-- Name: TABLE conference; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.conference TO api_user;


--
-- Name: TABLE institution; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT ON TABLE public.institution TO api_user;


--
-- PostgreSQL database dump complete
--

