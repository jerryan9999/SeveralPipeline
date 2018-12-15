# city = ['Seattle', 'Charlotte', 'Tampa', 'Orlando', 'Kissimmee','Jacksonville','Atlanta','Dallas','Phoenix','Denver']


select source_id from new_listings_investment where neighborhood_id is not null and neighborhood_id!=0 and cast(neighborhood_id as text) not like '999%' and beds is not null and baths is not null and rental_income_radio is not null and increase_radio is not null and score is not null and status=2 and pict_urls is not null and pict_urls is not null  and pict_urls!='{}' and  room_type in ('Single Family','Townhouse') and city in ('Seattle', 'Charlotte', 'Tampa', 'Orlando', 'Kissimmee','Jacksonville','Atlanta','Dallas','Phoenix','Denver')
