"""
Module for querying the publications.comprehensive_global table and returning
publications/works for a certain period.
"""

from db_connection import DBConnection

from_date = '2022-07-01'
to_date = '2023-06-30'

sql = f""" WITH cas_pubs AS (
            SELECT * FROM `publications`.`comprehensive_global_works` 
            WHERE institution_name = 'California Academy of Sciences')

            SELECT 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                GROUP_CONCAT(author_name SEPARATOR ', ') AS authors_concatenated

            FROM 
                cas_pubs

            GROUP BY 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year
                
            HAVING
                work_publication_date >= '{from_date}'
                AND work_publication_date <= '{to_date}';
                """

result = DBConnection.execute_query(sql)
print(result)
