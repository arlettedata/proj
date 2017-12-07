proj --in=county-fips-codes.csv --inheader=false --join=state-abbreviations.csv --joinheader=false where[right::{2}=={1}] County:{4} State:right::{1} {FIPS\ Code}:{2}\&{3} 
