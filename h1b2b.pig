data = LOAD '/home/hduser/test' USING PigStorage('\t') as 
(s_no:double,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:double,
year:chararray,
worksite:chararray,
longitude,
latitude);

H = foreach data generate case_status,year,worksite;

R = filter H by case_status == 'CERTIFIED';

B = group R by (worksite,year,case_status);

--STORE B INTO 'home/hduser/pig4' USING PigStorage('\t');

C = foreach B generate FLATTEN(group),COUNT(R.case_status) as number;





F0 = filter C by $1 == '2011';

D0 = order F0 by $3 desc;

G0 = limit D0 5;



F1 = filter C by $1 == '2012';

D1 = order F1 by $3 desc;

G1 = limit D1 5;




F2 = filter C by $1 == '2013';

D2 = order F2 by $3 desc;

G2 = limit D2 5;




F3 = filter C by $1 == '2014';

D3 = order F3 by $3 desc;

G3 = limit D3 5;




F4 = filter C by $1 == '2015';

D4 = order F4 by $3 desc;

G4 = limit D4 5;




F5 = filter C by $1 == '2016';

D5 = order F5 by $3 desc;

G5 = limit D5 5;



G = UNION G5,G4,G3,G2,G1,G0;

STORE G INTO 'home/hduser/H12b' USING PigStorage('\t');

--dump G;



