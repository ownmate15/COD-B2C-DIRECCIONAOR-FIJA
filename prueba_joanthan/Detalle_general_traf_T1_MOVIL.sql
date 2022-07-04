-- TRAFICO FIJO.


SELECT 
A.PERIODO,
A.CUENTAFACTURACIONCD AS CUENTA,
A.CLIENTECD AS CLIENTE,
A.NUMEROTELEFONO AS TELEFONO,
A.TIPOIDENTIFICACIONCD AS TIP_DOC,
A.NUMEROIDENTIFICACIONCD AS NUM_DOC,
B.FEC_EMI,
B.FEC_VEN
FROM DBI_PUBLIC.MR_PLANTAPOST

SELECT
A.PERIODO_PLANTA,
A.FEC_VEN,
B.NUMEROTELEFONO
FROM ACOB_B2C_T1_MOVIL_TARGET_FIN A
INNER JOIN DBI_PUBLIC.MR_PLANTAPOST B
ON A.PERIODO_PLANTA = B.PERIODO AND CAST(A.CLIENTE AS INT) = CAST(B.CLIENTECD AS INT)
--AND CAST(A.CUENTA AS INT) = CAST(B.CUENTAFACTURACIONCD AS INT)

/*
SELECT
A.PERIODO_PLANTA,
COUNT(*) Q,
count(distinct telefono)
FROM ACOB_B2C_T1_MOVIL_TARGET_FIN A
INNER JOIN DBI_PUBLIC.MR_PLANTAPOST B
ON A.PERIODO_PLANTA = B.PERIODO AND A.N_DOC = B.NUMEROIDENTIFICACIONCD
GROUP BY 1 ORDER BY 1 DESC;
*/


CALL SP_LVV_DROP_TABLE('ACOB_T1_NEW_POST');
CREATE TABLE ACOB_T1_NEW_POST
AS
(
SELECT
A.FEC_VEN,
CAST(A.CLIENTE AS INT) AS CLIENTE,
B.NUMEROTELEFONO AS TELEFONO
FROM ACOB_B2C_T1_MOVIL_TARGET_FIN A
INNER JOIN DBI_PUBLIC.MR_PLANTAPOST B
ON A.PERIODO_PLANTA = B.PERIODO AND CAST(A.CLIENTE AS INT) = CAST(B.CLIENTECD AS INT)
)
WITH DATA PRIMARY INDEX(FEC_VEN,TELEFONO,CLIENTE);

-- validacion:
select
fec_ven,count(*) q,
count(distinct telefono)
from ACOB_T1_NEW_POST
group by 1 order by 1 desc;

CALL SP_LVV_DROP_TABLE('ACOB_T1_NEW_POST_MUESTRA_1');
CREATE TABLE ACOB_T1_NEW_POST_MUESTRA_1
AS
(
SELECT * FROM ACOB_T1_NEW_POST
WHERE FEC_VEN = '2022-04-24'
SAMPLE 40
)
WITH DATA PRIMARY INDEX(FEC_VEN,TELEFONO,CLIENTE);



SELECT * FROM ACOB_T1_NEW_POST


---------------------------------------------------------------------------------------------------------------------------------------------------
--


sqoop import --direct  \
--connect jdbc:teradata://10.226.0.34/DBI_MIN \
--driver "com.teradata.jdbc.TeraDriver" \
--username ic_dgarciaau --password DavidGar#123 \
--table ACOB_T1_NEW_POST_MUESTRA_1 \
--hive-import  \
--hive-overwrite  \
--null-string '\\N' \
--null-non-string '\\N' \
--hive-table dev_perm.ACOB_T1_MOVIL_MUESTRA_TRAF_2 \
--m 8 \
--split-by fec_ven;


sqoop import --direct  \
--connect jdbc:teradata://10.226.0.34/DBI_MIN \
--driver "com.teradata.jdbc.TeraDriver" \
--username ic_dgarciaau --password DavidGar#123 \
--table ACOB_T1_NEW_POST \
--hive-import  \
--hive-overwrite  \
--null-string '\\N' \
--null-non-string '\\N' \
--hive-table dev_perm.ACOB_T1_MOVIL_MUESTRA_TRAF_FIN \
--m 8 \
--split-by fec_ven;

---------------------------------------------------------------------------------------------------------------------------------------------------

-- HIVE:


drop table if exists dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF;
create table dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF
(
cliente string,
telefono string
)
partitioned by (fec_ven date)
stored as orc tblproperties ("orc.compress"="SNAPPY");

-- # Insertando al historico.
INSERT OVERWRITE TABLE dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF PARTITION(fec_ven)
select
cliente,
telefono,
fec_ven
from dev_perm.ACOB_T1_MOVIL_MUESTRA_TRAF_FIN;

select * from dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF;

-------------------------------------------
-- HIVE
-- para 7 días.
select
'2022-03-05' a,
to_date(to_date('2022-03-05') - '1' month + '11' day) c,
to_date(to_date('2022-03-05') - '1' month + '17' day) d,
to_date(to_date('2022-03-05') - '2' month + '17' day) e,
to_date(to_date('2022-03-05') - '4' month + '17' day) f

## el trafico diario ahora será -1m + 20 dias.
# 7 dias
to_date(to_date(B.fec_ven) - '1' month + '11' day) and to_date(to_date(B.fec_ven) - '1' month + '17' day)

#30 dias
to_date(to_date(B.fec_ven) - '2' month + '17' day)  and to_date(to_date(B.fec_ven) - '1' month + '17' day)

#90 dias
to_date(to_date(B.fec_ven) - '4' month + '17' day)  and to_date(to_date(B.fec_ven) - '1' month + '17' day)




--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------- DATOS ----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------


select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TMIN TMIN_07,
a.TMB TMB_07
from
    (
    select
    B.cliente,
    B.fec_ven,
    B.telefono as telefono_A,
    sum(A.TMIN) TMIN,
    sum(A.TMB) TMB
    from dev_perm.LVV_AGREDATOS_COB A
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF B
    on A.telefono_A = B.telefono
    where A.fecha between to_date(to_date(B.fec_ven) - '1' month + '11' day) and to_date(to_date(B.fec_ven) - '1' month + '17' day)
    group by B.cliente,B.fec_ven, B.telefono
    ) a


select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TMIN TMIN_30,
a.TMB TMB_30
from
    (
    select
    B.cliente,
    B.fec_ven,
    B.telefono as telefono_A,
    sum(A.TMIN) TMIN,
    sum(A.TMB) TMB
    from dev_perm.LVV_AGREDATOS_COB A
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF B
    on A.telefono_A = B.telefono
    where A.fecha between to_date(to_date(B.fec_ven) - '2' month + '17' day)  and to_date(to_date(B.fec_ven) - '1' month + '17' day)
    group by B.cliente,B.fec_ven, B.telefono
    ) a

select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TMIN TMIN_90,
a.TMB TMB_90
from
    (
    select
    B.cliente,
    B.fec_ven,
    B.telefono as telefono_A,
    sum(A.TMIN) TMIN,
    sum(A.TMB) TMB
    from dev_perm.LVV_AGREDATOS_COB A
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF B
    on A.telefono_A = B.telefono
    where fecha between to_date(to_date(B.fec_ven) - '4' month + '17' day)  and to_date(to_date(B.fec_ven) - '1' month + '17' day)
    group by B.cliente,B.fec_ven, B.telefono
    ) a


                ########################
                ##### VOZ SALIENTE #####
                ########################
            

select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TMIN_V TMIN_V_07,
b.TTD TTD_07
from
    (
    select
    B.cliente,
    B.fec_ven,
    B.telefono as telefono_A,
    sum(A.TMIN_V) TMIN_V
    from dev_perm.LVV_AGRESAL_COB_01 A
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF B
    on A.telefono_A = B.telefono
    where A.fecha between to_date(to_date(B.fec_ven) - '1' month + '11' day) and to_date(to_date(B.fec_ven) - '1' month + '17' day)
    group by B.cliente,B.fec_ven, B.telefono
    ) a
left join
    (
    select 
    m.fec_ven,
    m.telefono_A,
    count(distinct m.telefono_B) TTD
    from 
        (
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from
        dev_perm.LVV_AGRESAL_COB_02 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '1' month + '11' day) and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        -----
        union
        -----
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from
        dev_perm.LVV_AGRESAL_COB_03 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '1' month + '11' day) and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        ) m
    group by m.fec_ven,m.telefono_A
    ) b
on a.telefono_A=b.telefono_A



select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TRE_V TRE_V_30,
a.TMIN_V_ON TMIN_V_ON_30,
a.TRE_104 TRE_104_30,
a.TRE_104_1245 TRE_104_1245_30,
b.TTD TTD_30
from
    (
    select
    Y.cliente,
    Y.fec_ven,
    Y.telefono AS telefono_A,
    sum(X.TRE_V) as TRE_V,
    sum(X.TMIN_V_ON) TMIN_V_ON,
    sum(X.TRE_104) as TRE_104,
    sum(X.TRE_104_1245) as TRE_104_1245
    from dev_perm.LVV_AGRESAL_COB_01 X
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
    on X.telefono_A = Y.telefono
    where X.fecha between to_date(to_date(Y.fec_ven) - '2' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
    group by Y.cliente, Y.fec_ven, Y.telefono
    ) a
left join
    (
    select 
    m.fec_ven,
    m.telefono_A,
    count(distinct m.telefono_B) TTD
    from 
        (
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from dev_perm.LVV_AGRESAL_COB_02 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '2' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        -----
        union
        ----- 
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from 
        dev_perm.LVV_AGRESAL_COB_03 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '2' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        ) m
    group by m.fec_ven, m.telefono_A
    ) b
on a.telefono_A = b.telefono_A



select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TRE_V TRE_V_90,
a.TMIN_V TMIN_V_90,
a.TRE_S TRE_S_90,
a.TRE_S_ON TRE_S_ON_90
from
    (
    select
    Y.cliente,
    Y.fec_ven,
    Y.telefono AS telefono_A,
    sum(TRE_V) as TRE_V,
    sum(TMIN_V) TMIN_V,
    sum(TRE_S) as TRE_S,
    sum(TRE_S_ON) as TRE_S_ON
    from dev_perm.LVV_AGRESAL_COB_01 X
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
    on X.telefono_A = Y.telefono
    where fecha between to_date(to_date(Y.fec_ven) - '4' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
    group by Y.cliente, Y.fec_ven, Y.telefono
    ) a


                ########################
                ##### VOZ ENTRANTE #####
                ########################

#7D
select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TRE_V_ON TRE_V_ON_07,
b.TTD TTD_07
from
    (
    select
    Y.cliente,
    Y.fec_ven,
    Y.telefono as telefono_A,
    sum(TRE_V_ON) as TRE_V_ON
    from dev_perm.LVV_AGREENT_COB_01 X
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
    on X.telefono_A = Y.telefono
    where fecha between to_date(to_date(Y.fec_ven) - '1' month + '11' day) and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
    group by Y.cliente, Y.fec_ven, Y.telefono
    ) a
left join
    (
    select 
    m.fec_ven,
    m.telefono_A,
    count(distinct m.telefono_B) TTD
    from 
        (
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from dev_perm.LVV_AGREENT_COB_02 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '1' month + '11' day) and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        ------
        union 
        ------
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from 
        dev_perm.LVV_AGREENT_COB_03 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '1' month + '11' day) and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        ) m
    group by m.fec_ven, m.telefono_A
    ) b
on a.telefono_A=b.telefono_A



select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TRE_V_ON_30D,
b.TTD TTD_30
from
    (
    select
    Y.cliente,
    Y.fec_ven,
    Y.telefono as telefono_A,
    sum(TRE_V_ON) as TRE_V_ON_30D
    from dev_perm.LVV_AGREENT_COB_01 X
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
    on X.telefono_A = Y.telefono
    where fecha between to_date(to_date(Y.fec_ven) - '2' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
    group by Y.cliente,Y.fec_ven, Y.telefono
    ) a
left join
    (
    select 
    m.fec_ven,
    m.telefono_A,
    count(distinct m.telefono_B) TTD
    from 
        (
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from 
        dev_perm.LVV_AGREENT_COB_02 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '2' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        ------
        union 
        ------
        select
        Y.fec_ven,
        X.telefono_A,
        X.telefono_B
        from 
        dev_perm.LVV_AGREENT_COB_03 X
        inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
        on X.telefono_A = Y.telefono
        where fecha between to_date(to_date(Y.fec_ven) - '2' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
        group by Y.fec_ven,X.telefono_A, X.telefono_B
        ) m
    group by m.fec_ven, m.Telefono_A
    ) b
on a.telefono_A=b.telefono_A


select
a.cliente,
a.fec_ven,
a.telefono_A,
a.TRE_V TRE_V_90
from
    (
    select
    Y.cliente,
    Y.fec_ven,
    Y.telefono as telefono_A,
    sum(TRE_V) as TRE_V
    from dev_perm.LVV_AGREENT_COB_01 X
    inner join dev_perm.ACOB_TEMP_T1_MOVILHIST_TRAF Y
    on X.telefono_A = Y.telefono
    where fecha between to_date(to_date(Y.fec_ven) - '4' month + '17' day)  and to_date(to_date(Y.fec_ven) - '1' month + '17' day)
    group by Y.cliente, Y.fec_ven, Y.telefono
    ) a




----------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ PROCESO TERADATA ----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT 
FEC_VEN,
COUNT(*) , COUNT(DISTINCT CLIENTE) Q2,
COUNT(DISTINCT TELEFONO_A) Q3
FROM ACOB_T1MOVIL_DATOS_7D
GROUP BY 1 ORDER BY 1

            --------------------------------
            ------------ DATOS -------------
            --------------------------------

-- 7 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_DATOS_7D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_DATOS_7D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_DATOS_7D,
AVG() AS AVG_DATOS_7D
FROM ACOB_T1MOVIL_DATOS_7D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);

-- 30 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_DATOS_30D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_DATOS_30D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_DATOS_30D,
AVG() AS AVG_DATOS_30D
FROM ACOB_T1MOVIL_DATOS_30D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);

-- 90 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_DATOS_90D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_DATOS_90D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_DATOS_90D,
AVG() AS AVG_DATOS_90D
FROM ACOB_T1MOVIL_DATOS_90D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);


            --------------------------------------
            ------------ VOZ SALIENTE -------------
            --------------------------------------

--7 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_VOZSAL_7D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_VOZSAL_7D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_VOZSAL_7D,
AVG() AS AVG_VOZSAL_7D
FROM ACOB_T1MOVIL_VOZSAL_7D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);

-- 30 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_VOZSAL_30D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_VOZSAL_30D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_VOZSAL_30D,
AVG() AS AVG_VOZSAL_30D
FROM ACOB_T1MOVIL_VOZSAL_30D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);

-- 90 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_VOZSAL_90D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_VOZSAL_90D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_VOZSAL_90D,
AVG() AS AVG_VOZSAL_90D
FROM ACOB_T1MOVIL_VOZSAL_90D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);

            --------------------------------------
            ------------ VOZ ENTRANTE ------------
            --------------------------------------


--7 DIAS
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_VOZENT_7D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_VOZENT_7D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_VOZENT_7D,
AVG() AS AVG_VOZENT_7D
FROM ACOB_T1MOVIL_VOZENT_7D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);

-- 30 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_VOZENT_30D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_VOZENT_30D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_VOZENT_30D,
AVG() AS AVG_VOZENT_30D
FROM ACOB_T1MOVIL_VOZENT_30D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);

-- 90 DIAS.
CALL DBI_MIN.SP_LVV_DROP_TABLE('ACOB_T1MOVIL_VOZENT_90D_AGRUP');
CREATE TABLE ACOB_T1MOVIL_VOZENT_90D_AGRUP
AS
(
SELECT
FEC_VEN,
CLIENTE,
SUM() AS SUM_VOZENT_90D,
AVG() AS AVG_VOZENT_90D
FROM ACOB_T1MOVIL_VOZENT_90D
GROUP BY 1,2
)
WITH DATA PRIMARY INDEX(FEC_VEN,CLIENTE);



