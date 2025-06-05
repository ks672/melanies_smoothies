--dora integration

use role accountadmin;
create or replace api integration dora_api_integration 
api_provider = aws_api_gateway 
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole' enabled = true 
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');




use role accountadmin;

create or replace external function util_db.public.grader(        
 step varchar     
 , passed boolean     
 , actual integer     
 , expected integer    
 , description varchar) 
 returns variant 
 api_integration = dora_api_integration 
 context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
 as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'  
;  


--DORA CHECK
select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from 
  ( SELECT 
  'DORA_IS_WORKING' as step
 ,(select 223) as actual
 , 223 as expected
 ,'Dora is working!' as description
); 



--lesson 1

--CREATE TABLE SMOOTHIES
CREATE OR REPLACE TABLE SMOOTHIES.PUBLIC.FRUIT_OPTIONS (
FRUIT_ID VARCHAR,
FRUIT_NAME VARCHAR
);


--CREATE FILE FORMAT
create file format smoothies.public.two_headerrow_pct_delim
   type = CSV,
   skip_header = 2,   
   field_delimiter = '%',
   trim_space = TRUE
;

--QUERY THE STAGE TO CHECK IF FILE IS OK
SELECT $1, $2, $3, $4, $5
FROM @smoothies.public.my_uploaded_files/fruits_available_for_smoothies
(FILE_FORMAT => smoothies.public.TWO_HEADERROW_PCT_DELIM);

--COPY THE FILES FROM STAGE TO SNOWFLAKE TABLE, THIS GIVES US AN ERROR ON THE COLUMN TYPE
COPY INTO smoothies.public.fruit_options
from @smoothies.public.my_uploaded_files
files = ('fruits_available_for_smoothies.txt')
file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
on_error = abort_statement
validation_mode = return_errors
purge = true;

--CHANGE THE COLUMN ORDERING USING SELECT STATEMENT
COPY INTO smoothies.public.fruit_options
from 
(SELECT $2 AS FRUID_ID, $1 AS FRUIT_NAME FROM @smoothies.public.my_uploaded_files/fruits_available_for_smoothies
(FILE_FORMAT => smoothies.public.TWO_HEADERROW_PCT_DELIM))

file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
on_error = abort_statement
--validation_mode = return_errors
purge = true;


SELECT * FROM smoothies.public.fruit_options;





--DORA CHECK
-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW001' as step
 ,( select count(*) 
   from SMOOTHIES.PUBLIC.FRUIT_OPTIONS) as actual
 , 25 as expected
 ,'Fruit Options table looks good' as description
);


select count(*)  from SMOOTHIES.PUBLIC.FRUIT_OPTIONS;



--lesson 2
CREATE OR REPLACE TABLE SMOOTHIES.PUBLIC.ORDERS
(
INGREDIENTS VARCHAR(200)
);

--insert into smoothies.public.orders(ingredients) values ('Apples Dragon Fruit Honeydew Guava Jackfruit ');

select * from smoothies.public.orders;

--GRANT ALL ON TABLE smoothies.public.orders TO SYSADMIN;

--TRUNCATE TABLE smoothies.public.orders;


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 'DABW002' as step
 ,(select IFF(count(*)>=5,5,0)
    from (select ingredients from smoothies.public.orders
    group by ingredients)
 ) as actual
 ,  5 as expected
 ,'At least 5 different orders entered' as description
);

--lesson 3
-- Set your worksheet drop lists

-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW003' as step
 ,(select ascii(fruit_name) from smoothies.public.fruit_options
where fruit_name ilike 'z%') as actual
 , 90 as expected
 ,'A mystery check for the inquisitive' as description
);


--lesson 4

alter table smoothies.public.orders add column name_on_order varchar(100);

select * from smoothies.public.orders;


alter table smoothies.public.orders add column order_filled boolean default false;

  update smoothies.public.orders
       set order_filled = true
       where name_on_order is null;


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW004' as step
 ,( select count(*) from smoothies.information_schema.columns
    where table_schema = 'PUBLIC' 
    and table_name = 'ORDERS'
    and column_name = 'ORDER_FILLED'
    and column_default = 'FALSE'
    and data_type = 'BOOLEAN') as actual
 , 1 as expected
 ,'Order Filled is Boolean' as description
);

--lesson 5

CREATE SEQUENCE ORDER_SEQ
    START = 1
    INCREMENT = 2
    ORDER
    COMMENT = 'Provide a unique id for each smoothie order';

truncate table smoothies.public.orders;

--Add the Unique ID Column  
alter table SMOOTHIES.PUBLIC.ORDERS 
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column

select * from smoothies.public.ORDERS;

grant all on table SMOOTHIES.PUBLIC.ORDERS to SYSADMIN;

GRANT ALL ON STREAMLIT "SMOOTHIES"."PUBLIC"."Custom Smoothie Order Form" TO SYSADMIN;

--MAKE SURE THE TABLE DEFINITON IS THE SAME 
create or replace table smoothies.public.orders (
       order_uid integer default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       constraint order_uid unique (order_uid),
       order_ts timestamp_ltz default current_timestamp()
);

DESC TABLE smoothies.public.orders;

select * from smoothies.public.orders at (timestamp => '2025-06-01 15:10:00');


-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW005' as step
 ,(select IFF(count(*)>=2, 2, 0) as num_sis_apps
    from (
        select count(*) as tally
        from snowflake.account_usage.query_history
        where query_text like 'execute streamlit%'
        group by query_text)
 ) as actual
 , 2 as expected
 ,'There seem to be 2 SiS Apps' as description
);


--lesson 7

set var1 = 2;
set var2 = 5;
set var3 = 7;

select $var1 + $var2 + $var3;

create function sum_mystery_bag_vars (var1 number, var2 number, var3 number)
    returns number as 'select var1+var2+var3';

select sum_mystery_bag_vars($var1, $var2, $var3);


-- Set your worksheet drop lists

-- Set these local variables according to the instructions
set this = -10.5;
set that = 2;
set the_other =  1000;

-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW006' as step
 ,( select util_db.public.sum_mystery_bag_vars($this,$that,$the_other)) as actual
 , 991.5 as expected
 ,'Mystery Bag Function Output' as description
);


set alternating_caps_phrase = 'aLtErNaTiNg CaPs!';

select initcap($alternating_caps_phrase);

create function neutralize_whining(phrase text)
    returns text as 'select initcap(phrase)';

select remove_whining($alternating_caps_phrase);

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW007' as step
 ,( select hash(neutralize_whining('bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy'))) as actual
 , -4759027801154767056 as expected
 ,'WHINGE UDF Works' as description
);

--lesson 8

--lesson 9
select * from orders order by order_ts desc;

--lesson 10
SELECT * FROM fruit_options;
alter table fruit_options add column SEARCH_ON VARCHAR;

UPDATE FRUIT_OPTIONS
SET 
SEARCH_ON = 
CASE WHEN FRUIT_NAME = 'Apples' THEN 'Apple'
     WHEN FRUIT_NAME = 'Blueberries' THEN 'Blueberry'
     WHEN FRUIT_NAME = 'Elderberries' THEN 'Elderberry'
     WHEN FRUIT_NAME = 'Raspberries' THEN 'Raspberry'
     WHEN FRUIT_NAME = 'Strawberries' THEN 'Strawberry'
     ELSE FRUIT_NAME END;


-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DABW008' as step 
   ,( select sum(hash_ing) from
      (select hash(ingredients) as hash_ing
       from smoothies.public.orders
       where order_ts is not null 
       and name_on_order is not null 
       and (name_on_order = 'Kevin' and order_filled = FALSE and hash_ing = 7976616299844859825) 
       or (name_on_order ='Divya' and order_filled = TRUE and hash_ing = -6112358379204300652)
       or (name_on_order ='Xi' and order_filled = TRUE and hash_ing = 1016924841131818535))
     ) as actual 
   , 2881182761772377708 as expected 
   ,'Followed challenge lab directions' as description
); 
