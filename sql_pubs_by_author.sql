-- Use a CTE that filters works based on affiliation = CAS or author has an orcid that is currently associated with CAS
WITH cas_pubs AS (
            SELECT * FROM `publications`.`comprehensive_global_works_v3` 
             WHERE institution_name = 'California Academy of Sciences'
             OR author_orcid in (SELECT author_orcid FROM authors where author_orcid != 'NULL' and author_active=1)
            )
            -- Since the original table separates publication into separate records based on institution, 
            -- need to group by work_id to reunite all the authors under one publication record
            SELECT 
                cas_pubs.author_id,
                cas_pubs.author_name,
                cas_pubs.author_raw_name,
                author_position,
                author_is_corresponding,
                authors.author_role,
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

            FROM 
                cas_pubs

            LEFT JOIN
            authors 
            ON cas_pubs.author_id = authors.author_alexid

            GROUP BY 
                cas_pubs.author_id,
                cas_pubs.author_name,
                cas_pubs.author_raw_name,
                author_position,
                author_is_corresponding,
                authors.author_role,
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

          

            ORDER BY cas_pubs.author_name;