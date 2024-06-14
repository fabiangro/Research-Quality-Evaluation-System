-- ==================================
-- MODYFING DATA OPERATIONS
-- ==================================
CREATE OR REPLACE PROCEDURE new_article(args jsonb)
LANGUAGE plpgsql AS $$
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
	

CREATE OR REPLACE PROCEDURE new_conference_points(args jsonb)
LANGUAGE plpgsql AS $$
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
-- ================================================
-- VIEWS
-- ================================================

CREATE OR REPLACE VIEW article_points AS
SELECT 
    a.id AS article_id,
    a.title,
    a.publication_date,
    a.conference,
    cp.points
FROM 
    article a
LEFT JOIN LATERAL (
	SELECT
		cp.points
	FROM
		conference_points cp
	WHERE
		cp.conference=a.conference AND
		cp.start_date <= a.publication_date
	ORDER BY
		cp.start_date DESC
	LIMIT 1
) cp ON (TRUE);


CREATE OR REPLACE VIEW article_details AS
SELECT 
    a.title,
    a.publication_date,
    a.conference,
    cp.points,
    aa.author,
    aa.institution,
    EXTRACT(YEAR FROM a.publication_date) AS year
FROM 
    article a
JOIN 
    conference_points cp ON a.conference = cp.conference
JOIN 
    article_author aa ON a.id = aa.article_id
WHERE 
    cp.start_date <= a.publication_date
ORDER BY 
    cp.start_date DESC;


-- =========================================
-- UNMODYFING DATA OPERATIONS
-- =========================================

CREATE OR REPLACE FUNCTION get_institution_points(args jsonb)
RETURNS TABLE (institutions VARCHAR(250), points NUMERIC)
LANGUAGE plpgsql AS $$
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

DROP FUNCTION get_author_points;

CREATE OR REPLACE FUNCTION get_author_points(args jsonb)
RETURNS TABLE (author VARCHAR(250), points BIGINT)
LANGUAGE plpgsql AS $$
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

DROP FUNCTION get_author_details;

CREATE FUNCTION get_author_details(args jsonb) 
RETURNS TABLE(conference character varying, year numeric, number_of_points integer, institution character varying, title character varying)
LANGUAGE plpgsql AS $$
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


-- ==============================================
-- TRIGGERS
-- ==============================================


CREATE OR REPLACE FUNCTION new_conf() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO conference (name) VALUES (NEW.conference) ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$ 
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER new_conference_from_points 
    BEFORE INSERT ON conference_points
    FOR EACH ROW 
    EXECUTE PROCEDURE new_conf();
	
	
CREATE OR REPLACE TRIGGER new_conference_from_article 
    BEFORE INSERT ON article
    FOR EACH ROW 
    EXECUTE PROCEDURE new_conf();




CREATE OR REPLACE FUNCTION new_aut() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO author ("name") VALUES (NEW.author) ON CONFLICT DO NOTHING;
	INSERT INTO institution ("name") VALUES (NEW.institution) ON CONFLICT DO NOTHING;
	RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER new_author
	BEFORE INSERT ON article_author
	FOR EACH ROW
	EXECUTE PROCEDURE new_aut();

-- ==================================================
-- TABLES
-- ==================================================


-- DROP TABLE IF EXISTS conference;
-- DROP TABLE IF EXISTS conference_points;
-- DROP TABLE IF EXISTS author;
-- DROP TABLE IF EXISTS institution;
-- DROP TABLE IF EXISTS article;
-- DROP TABLE IF EXISTS article_author;
-- DROP VIEW IF EXISTS article_points;


CREATE TABLE IF NOT EXISTS conference (
    name VARCHAR(255) PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS conference_points (
	conference VARCHAR(250) REFERENCES conference(name),
    start_date DATE NOT NULL,
    points INTEGER NOT NULL,
    PRIMARY KEY (conference, start_date)
);


CREATE TABLE IF NOT EXISTS author (
    name VARCHAR(255) PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS institution (
    name VARCHAR(255) PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS article (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    conference VARCHAR(250),
    publication_date DATE NOT NULL,
    UNIQUE (conference, title)
);


CREATE TABLE IF NOT EXISTS article_author (
    article_id INTEGER REFERENCES article(id),
    author VARCHAR(250) REFERENCES author(name),
    institution VARCHAR(250) REFERENCES institution(name),
    UNIQUE (author, article_id)
);





