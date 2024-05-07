# publications_finder

Use cases:

* Find all CAS papers in a given year:    
Set `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.   
Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `False`.  
Run `python queries.py`  

* Find all CAS papers only from curators in a given year or year range.
Set `[years]` in [config.ini](config.ini). Set both `TO_DATE` and `FROM_DATE` to the same value for a single year.  
Set `single_authors` under `[query_results]` in [config.ini](config.ini) to `True`.  
Set `curators` under `[query_results]` in [config.ini](config.ini) to `True`.
Run `python queries.py`

* find all cas papers only from non-curators in a given year or year range

Find all affiliated authors in a given year
 
What goals [areas] we are addressing with collections based research
 * list papers by goals

Generate stats? Not sure what would go here.

derived citation index (how cited each paper was) - shoud also be able to query for earlier years

by department (picking up aquarium, planetarium, etc)

Expoirt all these in a tabulated data form

list of journals we are pubnlising in for a given year or year range

ratio/l;ist of open vs closed journals

send out emails on a <interval to be determined> containting.. (stuff fromt the above?)
 - potentially this is a bad idea; people will always want to edit. Maybe we auto-push to quarterly
 - and annual sheets that people can edit. Create a workflow where people do a google form to add thier 
   work? or just have them splat it into the sheet? TBD, good times.
 - maricela: make a custom form that has data validation built in? pevent bad data and weird dates, etc. 




