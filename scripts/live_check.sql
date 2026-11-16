-- Opt-in live check (make live-check): keyless relations against the real upstreams.
-- Downloads ~20 MB into a throwaway cache. Not part of make check.
LOAD vgi;
SELECT 'members' AS relation, count(*) AS rows FROM us_congress.main.members;
SELECT 'longest_serving_senators' AS check, bioguide_id, sum(end_date - start_date) AS days
FROM us_congress.main.member_terms WHERE chamber = 'senate' GROUP BY bioguide_id ORDER BY days DESC LIMIT 5;
SELECT 'women_in_119th_house' AS check, count(DISTINCT t.bioguide_id) AS women
FROM us_congress.main.member_terms t JOIN us_congress.main.members m USING (bioguide_id)
WHERE t.chamber = 'house' AND m.gender = 'F' AND t.start_date < DATE '2027-01-03' AND t.end_date > DATE '2025-01-03';
SELECT 'senate_rollcalls_118' AS check, count(*) AS votes FROM us_congress.main.rollcalls(118, chamber := 'senate');
SELECT 'most_conservative_house_119' AS check, bioguide_id, nominate_dim1
FROM us_congress.main.member_ideology(119, chamber := 'house') ORDER BY nominate_dim1 DESC NULLS LAST LIMIT 1;
SELECT 'senate_votes_119_1' AS check, count(*) AS votes, max(vote_date) AS latest FROM us_congress.main.senate_votes(119, 1);
SELECT 'senate_vote_positions' AS check, count(*) AS senators, count(bioguide_id) AS mapped
FROM us_congress.main.senate_vote_positions(119, 1, 1);
SELECT 'member_votes_s119' AS check, count(*) AS rows, count(bioguide_id) AS with_bioguide, count(DISTINCT rollnumber) AS rollcalls
FROM us_congress.main.member_votes(119, chamber := 'senate');
SELECT 'cache' AS check, count(*) AS files, sum(bytes) AS bytes FROM us_congress.main.cache_files;
