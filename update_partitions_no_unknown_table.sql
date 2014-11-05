--
-- Update_partitions - Takes a begin time, schema name, primary (parent) table name,
--                     table owner, the name of the date column,
--                     and if we want 'week'ly or 'month'ly partitions.
--                     The number of created tables is returned.
--                     ex: SELECT public.create_date_partitions_for_table('2010-02-01','my_schema','my_data','postgres','create_date','week',1,true,true)
--

-- Function: public.create_date_partitions_for_table(timestamp without time zone, text, text, text, text, text, integer, boolean, boolean)

-- DROP FUNCTION public.create_date_partitions_for_table(timestamp without time zone, text, text, text, text, text, integer, boolean, boolean);

CREATE OR REPLACE FUNCTION public.create_date_partitions_for_table(begin_time timestamp without time zone, schema_name text, primary_table_name text, table_owner text, date_column text, plan text, step integer, fill_child_tables boolean, truncate_parent_table boolean)
  RETURNS integer AS
$BODY$
declare startTime timestamp;
declare endTime timestamp;
declare intervalTime timestamp;
declare createStmts text;
declare insertStmts text;
declare createTrigger text;
declare fullTablename text;
declare triggerName text;
declare createdTables integer;
declare dateFormat text;
declare planInterval interval;
 
BEGIN
dateFormat:=CASE WHEN plan='month' THEN 'YYYYMM'
                 WHEN plan='week'  THEN 'IYYYIW'
                 WHEN plan='day'   THEN 'YYYYDDD'
         WHEN plan='year'  THEN 'YYYY'
                 ELSE 'error'
            END;
IF dateFormat='error' THEN
  RAISE EXCEPTION 'Plan % no valido (valores validos son month,week,day,year)', plan;
END IF;
-- Store the incoming begin_time, and set the endTime to one month/week/day in the future 
--     (this allows use of a cronjob at any time during the month/week/day to generate next month/week/day's table)
startTime:=(date_trunc(plan,begin_time));
planInterval:=(step||' '||plan)::interval;
endTime:=(date_trunc(plan,(current_timestamp + planInterval)));
createdTables:=0;   

-- Begin creating the trigger function
createTrigger:='CREATE OR REPLACE FUNCTION '||schema_name||'.trf_'||primary_table_name||'_insert_trigger_function()
                RETURNS TRIGGER AS $$
    declare startTime timestamp;
    declare intervalTime timestamp;
    declare fullTablename text;
    declare insertStatment text;
    declare createTableStatment text;
    BEGIN
    ';
     
while (startTime <= endTime) loop
 
   fullTablename:=primary_table_name||'_'||to_char(startTime,dateFormat);
   intervalTime:= startTime + planInterval;

   -- The table creation sql statement
   if not exists(select * from information_schema.tables where table_schema = schema_name AND table_name = fullTablename) then
     createStmts:='CREATE TABLE '||schema_name||'.'||fullTablename||' (
              CHECK ('||date_column||' >= '''||startTime||''' AND '||date_column||' < '''||intervalTime||''')
              ) INHERITS ('||schema_name||'.'||primary_table_name||')';    
 
     -- Run the table creation
     EXECUTE createStmts;
    
     -- Set the table owner     
     createStmts :='ALTER TABLE '||schema_name||'.'||fullTablename||' OWNER TO '||table_owner||';';
     EXECUTE createStmts;     
  
     -- Create an index on the timestamp     
     createStmts:='CREATE INDEX idx_'||fullTablename||'_'||date_column||' ON '||schema_name||'.'||fullTablename||' ('||date_column||');';
     EXECUTE createStmts;
     
     RAISE NOTICE 'Child table %.% created',schema_name,fullTablename;
    
     --if fill_child_tables is true then we fill the child table with the parent's table data that satisfies the child's table check constraint
     IF (fill_child_tables) THEN
        RAISE NOTICE 'Filling child table %.%',schema_name,fullTablename;
        insertStmts:='INSERT INTO '||schema_name||'.'||fullTablename||' (
            SELECT * FROM '||schema_name||'.'||primary_table_name||' 
            WHERE '||date_column||' >= '''||startTime||''' AND 
                  '||date_column||' < '''||intervalTime||'''
              );';
        EXECUTE insertStmts;
     END IF;

     -- Track how many tables we are creating (should likely be 1, except for initial run and backfilling efforts).
     createdTables:=createdTables+1;
   end if;
   
   startTime:=intervalTime;
      
end loop;
-- Finish creating the trigger function
-- The UNDEFINED_TABLE exception is captured on child table is created 'on-the-fly' when new data arrives and 
-- no partition is created to match this data criteria
createTrigger:=createTrigger || '
    fullTablename  := '''||primary_table_name||'_''||'||'to_char(NEW.'||date_column||','''||dateFormat||''');
    insertStatment := ''INSERT INTO '||schema_name||'.'''||'||fullTablename||'' VALUES ($1.*)'';
    BEGIN
        --Try insert on appropiatte child table if exists
        EXECUTE insertStatment using NEW;
        --When child tables not exists, generate it on the fly
        EXCEPTION WHEN UNDEFINED_TABLE THEN
            startTime:=(date_trunc('''||plan||''',NEW.'||date_column||'));
            intervalTime  := startTime + ('''||step||' '||plan||''')::interval;  

            createTableStatment:=''CREATE TABLE '||schema_name||'.''||fullTablename||'' (
                  CHECK ('||date_column||' >= ''''''||startTime||'''''' AND '||date_column||' < ''''''||intervalTime||'''''')
                  ) INHERITS ('||schema_name||'.'||primary_table_name||')'';    
            EXECUTE createTableStatment;

            createTableStatment :=''ALTER TABLE '||schema_name||'.''||fullTablename||'' OWNER TO '||table_owner||';'';
            EXECUTE createTableStatment;

            createTableStatment:=''CREATE INDEX idx_''||fullTablename||''_'||date_column||' ON '||schema_name||'.''||fullTablename||'' ('||date_column||');'';
            EXECUTE createTableStatment;    

            --Try the insert again, now the table exists
            EXECUTE insertStatment using NEW;
        WHEN OTHERS THEN        
            RAISE EXCEPTION ''Error en trigger'';
            RETURN NULL;
    END;
    RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;';

EXECUTE createTrigger;

-- Create the trigger that uses the trigger function, if it isn't already created
triggerName:='tr_'||primary_table_name||'_insert_trigger'; 
IF NOT EXISTS(select * from information_schema.triggers where trigger_name = triggerName) then
  createTrigger:='CREATE TRIGGER tr_'||primary_table_name||'_insert_trigger
                  BEFORE INSERT ON '||schema_name||'.'||primary_table_name||' 
                  FOR EACH ROW EXECUTE PROCEDURE '||schema_name||'.trf_'||primary_table_name||'_insert_trigger_function();';
  EXECUTE createTrigger;
END IF;

-- If truncate_parent_table parameter is true, we truncate only the parent table data as this data is in child tables
IF (truncate_parent_table) THEN
    RAISE NOTICE 'Truncate ONLY parent table %.%',schema_name,primary_table_name;
    insertStmts:='TRUNCATE TABLE ONLY '||schema_name||'.'||primary_table_name||';';
        EXECUTE insertStmts;
END IF;


RETURN createdTables;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.create_date_partitions_for_table(timestamp without time zone, text, text, text, text, text, integer, boolean, boolean)
  OWNER TO postgres;
COMMENT ON FUNCTION public.create_date_partitions_for_table(timestamp without time zone, text, text, text, text, text, integer, boolean, boolean) IS 'The function is created in the public schema and is owned by user postgres.
The function takes params:
begin_time          - Type: timestamp - Desc: time of your earliest data. This allows for backfilling and for reducing trigger function overhead by avoiding legacy date logic.
schema_name         - Type: text      - Desc: name of the schema that contains the parent table. Child tables are created here.
primary_table_name  - Type: text      - Desc: name of the parent table. This is used to generate monthly tables ([primary_table_name]_YYYYMM) and an unknown table ([primary_table_name]_unknowns). It is also used in the trigger and trigger function names.
table_owner         - Type: text      - Desc: name of PostgreSQL Role to be assigned as owner of the child tables.
date_column         - Type: text      - Desc: name of the timestamp/date column that is used for check constraints and insert trigger function.
plan                - Type: text      - Desc: how to implement the partition, valid values are day,week,month,year.
step                - Type: integer   - Desc: the step taken by the plan (if you want bimestral partition you put plan month and step 2)
fill_child_tables   - Type: boolean   - Desc: if you want to load data from parent table to each child tables.
truncate_parent_table   - Type: boolean   - Desc: if you want to delete table of the parent table

Considerations:

- The insert trigger function is recreated everytime you run this function.

- If child tables already exist, the function simply updates the trigger 
function and moves to the next table in the series.

- This function does not raise exceptions when errant data is encountered.
The trigger captures the UNDEFINED_TABLE exception when any data that does 
not have a matching child table and it automatically generates 
the appropiate child table and insert the row that generated the exeception.

- The function returns the number of tables that it created.

- The fill_child_tables and truncate_parent_table must be used carefully you may
respald your parent table data before
';
