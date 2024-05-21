# publications_finder

## Create the MySQL database and ingest publication data from OpenAlex. 
1. Clone this repository. 
2. cd to repository location and run `create_database.sh` (make sure this is first step. can't remember)  
3. Set config options in config.ini. These include:  
  * institutional ROR id (required)  
  * the email to be used for OpenAlex requests to get into the polite pool (optional, but recommended)  
  * ORCIDS of researchers to explicitly search for (useful when authors are missed by institutional affiliation search) (optional)  
* the from_year and to_year to bound query results (optional)  
4. Run `python main.py`
*** need to add option for just inserting new records***

## Set up authors table in MySQL database & populate with records from a spreadsheet.  
1. Copy filepath of local spreadsheet with author records.  
2. Copy the name of the Docker container.
3. Use that filepath as the --local_sheet_path argument and the Docker container name as the --container_name argument in the following commands.  
4. To load all the author data for the first time into the authors database table:  
`python populate_authors.py --local_sheet_path [your path] --container_name [container name] --load_data`  
5. To update the authors database with modified records from the authors spreadsheet:  
`python populate_authors.py --local_sheet_path [your path] --container_name [container name] --update_data`  

## Use cases:

### Find all CAS affiliated papers during a year interval or single year:    
1. Set `TO_DATE` and `FROM_DATE` under `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.   
2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `False`.    
3. Run `python queries.py` 

Run `python queries.py --from_year [year] --to_year [year]` e.g. `python queries.py --from_year 2022 --to_year 2022`

### Find all CAS papers only from curators in a given year or year range.  
1. Set `TO_DATE` and `FROM_DATE` under `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.    
2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `True`.  
3. Set `curators` under `[query_results]` in [config.ini](config.ini) to `True`.
4. Run `python queries.py`

### Find all CAS papers only from non-curators in a given year or year range.  
1. Set `TO_DATE` and `FROM_DATE` under `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.    
2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `True`.  
3. Set `curators` under `[query_results]` in [config.ini](config.ini) to `False`.  
4. Run `python queries.py`  

### Find publications by department (picking up aquarium, planetarium, etc)  
 1. Set `TO_DATE` and `FROM_DATE` under `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.    
 2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `True`.   
 3. Set `curators` under `[query_results]` in [config.ini](config.ini) to `False`.  
 4. Set `department` to one of the allowed values under `[query_results]` in [config.ini](config.ini).
 Allowed values:
  Anthropology  
  Aquarium  
  Botany  
  Center for Biodiversity and Community Science  
  Center for Comparative Genomics  
  Center for Exploration and Travel Health  
  Coral Regeneration Lab  
  Education  
  Entomology  
  Herpetology  
  Ichthyology  
  iNaturalist  
  Invertebrate Zoology and Geology  
  Microbiology  
  Ornithology and Mammalogy  
  Planetarium  
  Scientific Computing  

 5. Run `python queries.py`  

### Toggle a csv of journal information for any of the queries above.
(Saves a csv sorted by counts and also prints a count of papers by publisher + journal).  
1. Keep options for desired query.  
2. Set `journal_info` under `[query_results]` in [config.ini](config.ini) to `True`.   
3. Run `python queries.py`

### Toggle a csv of UN sustainability goals for any of the queries above.
What goals are we addressing with collections based research?
 (Saves a csv sorted by goal counts and also prints a count of papers by goal).
 1. Keep options for desired query.  
 2. Set `sustainable_goals` under `[query_results]` in [config.ini](config.ini) to `True`.  
 3. Run `python queries.py`    


### Proportion of open access vs closed access publications.  



### Send out emails containing the above csv results.  
options for `send_email.py` in progress. 



 - potentially this is a bad idea; people will always want to edit. Maybe we auto-push to quarterly
 - and annual sheets that people can edit. Create a workflow where people do a google form to add thier 
   work? or just have them splat it into the sheet? TBD, good times.
 - maricela: make a custom form that has data validation built in? pevent bad data and weird dates, etc. 




