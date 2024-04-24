-- Use a CTE that filters works based on affiliation = CAS or author has an orcid that is currently associated with CAS
WITH cas_pubs AS (
            SELECT * FROM `publications`.`comprehensive_global_works_v3` 
             WHERE institution_name = 'California Academy of Sciences'
             OR author_orcid in (SELECT author_orcid FROM authors where author_orcid != 'NULL' and author_active=1)
            )
            -- Since the original table separates publication into separate records based on institution, 
            -- need to group by work_id to reunite all the authors under one publication record
            SELECT 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                GROUP_CONCAT(DISTINCT author_name SEPARATOR ', ') AS authors_concatenated,
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count

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
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count
                
            --  Filter on groups for a certain time period
             HAVING
                 work_publication_year = '2022'
                -- AND work_publication_date <= '2023-06-30'   

            ORDER BY authors_concatenated;