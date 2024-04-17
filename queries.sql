

WITH cas_pubs AS (
            SELECT * FROM `publications`.`comprehensive_global_works_v2` 
             WHERE institution_name = 'California Academy of Sciences'
            
            OR author_orcid in (SELECT author_orcid FROM authors where author_orcid != 'NULL' and author_active=1))

            SELECT 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                GROUP_CONCAT(DISTINCT author_name SEPARATOR ', ') AS authors_concatenated,
                work_sustainable_dev_goal

            FROM 
                cas_pubs

            GROUP BY 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal
                
             HAVING
                 work_publication_year = '2022'
                -- AND work_publication_date <= '2023-06-30'
                

            ORDER BY authors_concatenated;