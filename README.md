# publications_finder

Use cases:

* Find all CAS papers in a given year:    
1. Set `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.   
2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `False`.  
3. Run `python queries.py`  

* Find all CAS papers only from curators in a given year or year range.  
1. Set `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.  
2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `True`.  
3. Set `curators` under `[query_results]` in [config.ini](config.ini) to `True`.
4. Run `python queries.py`

* Find all CAS papers only from non-curators in a given year or year range.  
1. Set `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.  
2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `True`.  
3. Set `curators` under `[query_results]` in [config.ini](config.ini) to `False`.  
4. Run `python queries.py`


* Find all affiliated authors in a given year  
Same instructions as Find all CAS papers in a given year.  
 
* What goals [areas] we are addressing with collections based research?
 * List papers by goals:
 1. Set `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.  
 2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `False`.  
 3. Set `curators` under `[query_results]` in [config.ini](config.ini) to `False`.  
 4. Set `sustainable_goals` under `[query_results]` in [config.ini](config.ini) to `True`. 
 5. Run `python queries.py`    

* Find publications by department (picking up aquarium, planetarium, etc)  
 1. Set `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.  
 2. Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `True`.  
 3. Set `curators` under `[query_results]` in [config.ini](config.ini) to `False` for all roles, `True` for curators only.  
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


Generate stats? Not sure what would go here.

derived citation index (how cited each paper was) - shoud also be able to query for earlier years



list of journals we are pubnlising in for a given year or year range

ratio/l;ist of open vs closed journals

send out emails on a <interval to be determined> containting.. (stuff fromt the above?)
 - potentially this is a bad idea; people will always want to edit. Maybe we auto-push to quarterly
 - and annual sheets that people can edit. Create a workflow where people do a google form to add thier 
   work? or just have them splat it into the sheet? TBD, good times.
 - maricela: make a custom form that has data validation built in? pevent bad data and weird dates, etc. 




