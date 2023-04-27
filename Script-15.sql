start transaction;
-- =======================================
-- neue SWCLs
-- =======================================
DROP TEMPORARY TABLE IF EXISTS temp_swcl;
CREATE TEMPORARY TABLE temp_swcl
with cte_vals as
(
    select
        'V04.007.003.RS' as tnr,
        'HCP5_DK1V_P4' as name,
        'HCP5_DK1V_P4' as abbreviation,
        '0x7E5' as diag,
        'Wael Bibari (TH-71)' as ld,
        'CARIAD' as ldm,
        'TH-71' as lda,
        'ESR Labs' as supplier,
        sg
    from 
    (
        select
            'SG475' as sg
        union all
        select
            'SG476' as sg
        union all
        select
            'SG477' as sg
        union all
        select
            'SG478' as sg
    ) sg
    union all
    select
        'V04.007.003.RR' as tnr,
        'HCP5_DK1V_P2' as name,
        'HCP5_DK1V_P2' as abbreviation,
        '0x7E6' as diag,
        'Wael Bibari (TH-71)' as ld,
        'CARIAD' as ldm,
        'TH-71' as lda,
        'ESR Labs' as supplier,
        sg
    from 
    (
        select
            'SG475' as sg
        union all
        select
            'SG476' as sg
        union all
        select
            'SG477' as sg
        union all
        select
            'SG478' as sg
    ) sg
)
select
    LEFT(REPLACE(REPLACE(tnr, '.', ''), ' ', ''), 9) as tnr,
    SUBSTR(REPLACE(REPLACE(tnr, '.', ''), ' ', ''), 9) as tnr_index, 
    name,
    abbreviation,
    diag,
    ld,
    ldm,
    lda,
    supplier,
    sg,
    DENSE_RANK() over (order by tnr) + (SELECT MAX(id) from tnr) as tnr_id,
    DENSE_RANK() over (order by tnr) + (SELECT MAX(id) from swcls) as swcl_id,
    da.id as da_id,
    p.id as person_id,
    REPLACE(sg, 'SG', '') as sg_id
from cte_vals
left join diagnosis_addresses da 
    on da.diagnosis_address = REPLACE(diag, '0x', '')
left join people p 
    on CONCAT(p.first_name, ' ', p.last_name, ' (', p.department, ')') = ld;

insert into swcls
(
    id,
    `type`,
    name,
    abbreviation,
    supplier,
    created_at,
    updated_at,
    st12 
)
select distinct 
    swcl_id,
    'SWCL',
    name,
    abbreviation,
    supplier,
    now(),
    now(),
    0.9
from temp_swcl;

insert into da_swcls
(
    swcl_id,
    da_id,
    st12
)
select distinct
    swcl_id,
    da_id,
    0.9
from temp_swcl;

insert into swcls_people_roles
(
    swcl_id,
    person_id,
    created_at,
    updated_at,
    `position`,
    st12
)
select distinct
    swcl_id,
    person_id,
    now(),
    now(),
    'lead',
    0.9
from temp_swcl;

insert into sg_swcls  
(
    sg_id,
    swcl_id,
    st12,
    `uuid`
)
select distinct
    sg_id,
    swcl_id,
    0.9,
    UUID() 
from temp_swcl;

-- =======================================
-- neue SWCs
-- =======================================

DROP TEMPORARY TABLE IF EXISTS temp_swcl;
CREATE TEMPORARY TABLE temp_swcl
with cte_vals as
(
    select
        '1393' as ce_editor,
        'External Device Interface Audi' as name,
        'EDI' as abbreviation,
        'Hibler, Daniel' as ld,
        'CARIAD' as ldm,
        'T2-BD' as lda,
        'Quartett Mobile' as supplier,
        '4430090' as fuli,
        'n/a' as sg
    union all
    select
        '1392' as ce_editor,
        'External Device Interface Porsche' as name,
        'EDI' as abbreviation,
        'Hibler, Daniel' as ld,
        'CARIAD' as ldm,
        'T2-BD' as lda,
        'Quartett Mobile' as supplier,
        '4430090' as fuli,
        'n/a' as sg
    union all
    select
        'CE1457v1' as ce_editor,
        'TV Streaming App' as name,
        'TV Streaming App' as abbreviation,
        'Christian Winter' as ld,
        'CARIAD' as ldm,
        'T2-BF' as lda,
        'Xperi Inc.' as supplier,
        '4131146' as fuli,
        'n/a' as sg
)
select
    name,
    abbreviation,
    ce_editor,
    ld,
    ldm,
    lda,
    supplier,
    sg,
    fuli,
    DENSE_RANK() over (order by ce_editor) + (SELECT MAX(id) from swcs) as swc_id,
    p.id as person_id,
    REPLACE(sg, 'SG', '') as sg_id
from cte_vals
left join people p 
    on CONCAT(p.first_name, ' ', p.last_name) = ld
    or CONCAT(p.last_name , ', ', p.first_name) = ld;
    
-- =======================================
-- update SWCs
-- =======================================

drop temporary table if exists temp_rel_swc;
CREATE TEMPORARY TABLE temp_rel_swc
with cte_swc as
(
    select 
        'HCP5_DK1V_P2' as swcl,
        121 as swcl_old_id,
        swc
    from 
    (
        SELECT 'SWC48' as swc
        union all
        SELECT 'SWC133' as swc
        union all
        SELECT 'SWC134' as swc
        union all
        SELECT 'SWC138' as swc
        union all
        SELECT 'SWC139' as swc
        union all
        SELECT 'SWC151' as swc
        union all
        SELECT 'SWC152' as swc
        union all
        SELECT 'SWC433' as swc
        union all
        SELECT 'SWC447' as swc
        union all
        SELECT 'SWC694' as swc
        union all
        SELECT 'SWC695' as swc
        union all
        SELECT 'SWC723' as swc
        union all
        SELECT 'SWC793' as swc
        union all
        SELECT 'SWC911' as swc
        union all
        SELECT 'SWC914' as swc
        union all
        SELECT 'SWC915' as swc
        union all
        SELECT 'SWC936' as swc
        union all
        SELECT 'SWC1226' as swc
    )as s
    union all
    select 
        'HCP5_DK1V_P4' as swcl,
        122 as swcl_old_id,
        swc
    from 
    (
        select 'SWC1290' as swc
        union all
        select 'SWC979' as swc
        union all
        select 'SWC854' as swc
        union all
        select 'SWC137' as swc
        union all
        select 'SWC740' as swc
        union all
        select 'SWC984' as swc
        union all
        select 'SWC153' as swc
        union all
        select 'SWC981' as swc
        union all
        select 'SWC982' as swc
        union all
        select 'SWC983' as swc
        union all
        select 'SWC140' as swc
        union all
        select 'SWC980' as swc
    )as s
)
select
    swcl,
    swc,
    swcl_old_id,
    REPLACE(swc, 'SWC', '') as swc_id,
    s.id as swcl_id
from cte_swc
left join swcls s
    on s.name = swcl;

DROP TEMPORARY TABLE IF EXISTS temp_update_rel;
CREATE TEMPORARY TABLE temp_update_rel
select distinct ss.*
from swcs_swcls ss 
inner join temp_rel_swc t
    on t.swcl_old_id = ss.swcl_id
    and t.swc_id = ss.swc_id; 

DROP TEMPORARY TABLE IF EXISTS temp_other_rel;
CREATE TEMPORARY TABLE temp_other_rel
select distinct ss.*
from swcs_swcls ss 
inner join temp_rel_swc t
    on t.swcl_old_id = ss.swcl_id
    and t.swc_id <> ss.swc_id; 

SELECT distinct fss.funktion_id 
from fkt_swcs_swcls fss
where 
    fss.swcs_swcls_id in (select u.id from temp_update_rel u)
    and not EXISTS 
    (
        select *
        from fkt_swcs_swcls fss2
        where fss2.funktion_id = fss.funktion_id
        and fss2.swcs_swcls_id in (select u.id from temp_other_rel u)
    );
    
update swcs_swcls ss
inner join temp_rel_swc t
    on t.swcl_old_id = ss.swcl_id
    and t.swc_id = ss.swc_id
set ss.swcl_id = t.swcl_id;


INSERT into functions_components 
(
funktions_id,
`table`,
component_id,
created_at,
updated_at,
kpb_id,
cluster_id,
country_id
)
select distinct
    fss.funktion_id,
    'swcls',
    ss.swcl_id,
    now(),
    now(),
    1,
    1,
    1
from fkt_swcs_swcls fss
inner join swcs_swcls ss 
    on ss.id = fss.swcs_swcls_id 
where ss.id in (select u.id from temp_update_rel u);

-- =======================================
-- set people
-- =======================================

DROP TEMPORARY TABLE IF EXISTS temp_people;
CREATE TEMPORARY TABLE temp_people
with cte_p as 
(
    SELECT 'Mikolajczyk' as last_name, 'Marta' as first_name, 'CARIAD' as brand, 'T1-73' as abteilung union all
    SELECT 'Herberth' as last_name, 'Roland' as first_name, 'Porsche' as brand, '  EGE5' as abteilung union all
    SELECT 'Weitensfelder' as last_name, 'Christin' as first_name, 'CARIAD' as brand, 'T1-73' as abteilung union all
    SELECT 'Mukherjee' as last_name, 'Annwesh' as first_name, 'CARIAD' as brand, 'TH-82' as abteilung union all
    SELECT 'Sandner' as last_name, 'Christian' as first_name, 'CARIAD' as brand, 'TH-84' as abteilung union all
    SELECT 'Anders' as last_name, 'Fabian' as first_name, 'CARIAD' as brand, 'TH-83' as abteilung union all
    SELECT 'Depner' as last_name, 'Daniel' as first_name, 'CARIAD' as brand, 'TH-82' as abteilung union all
    SELECT 'Koeppl' as last_name, 'Florian' as first_name, 'CARIAD' as brand, 'T2-BF' as abteilung union all
    SELECT 'Klein' as last_name, 'Markus' as first_name, 'CARIAD' as brand, 'TH-86' as abteilung union all
    SELECT 'Chittoju' as last_name, 'Ravi' as first_name, 'CARIAD' as brand, 'TH-72' as abteilung union all
    SELECT 'Kaiser' as last_name, 'Shoma-Jakob' as first_name, 'CARIAD' as brand, 'T1-23' as abteilung union all
    SELECT 'Heidenfelder' as last_name, 'Jonas' as first_name, 'CARIAD' as brand, 'TH-85' as abteilung union all
    SELECT 'Puetz' as last_name, 'Simon' as first_name, 'CARIAD' as brand, 'T2-AJ' as abteilung union all
    SELECT 'Prankl' as last_name, 'Wolfgang' as first_name, 'CARIAD' as brand, 'T1-73' as abteilung union all
    SELECT 'Salich' as last_name, 'Sebastian' as first_name, 'CARIAD' as brand, 'TH-82' as abteilung union all
    SELECT 'Shivananjaiah' as last_name, 'Sudarshan' as first_name, 'CARIAD' as brand, 'T2-BD' as abteilung union all
    SELECT 'Otto' as last_name, 'Oliver' as first_name, 'CARIAD' as brand, 'T2-BC' as abteilung union all
    SELECT 'Carls' as last_name, 'Carl-Aron' as first_name, 'AUDI' as brand, 'I/EI-11' as abteilung union all
    SELECT 'Cao' as last_name, 'Iijia' as first_name, 'CARIAD China' as brand, 'C|TB-13' as abteilung union all
    SELECT 'Koch' as last_name, 'Ronald' as first_name, 'CARIAD' as brand, 'T2-AE' as abteilung union all
    SELECT 'Roehlre' as last_name, 'Alexander' as first_name, 'CARIAD' as brand, 'T2-AA' as abteilung union all
    SELECT 'Ejaz' as last_name, 'Muhammad Moeed' as first_name, 'CARIAD' as brand, 'TH-83' as abteilung union all
    SELECT 'Hibler' as last_name, 'Daniel' as first_name, 'CARIAD' as brand, 'T2-BD' as abteilung union all
    SELECT 'Boesl' as last_name, 'Mathias' as first_name, 'CARIAD' as brand, 'T2-AI' as abteilung union all
    SELECT 'Auhuber' as last_name, 'Daniel' as first_name, 'CARIAD' as brand, 'T2-BB' as abteilung union all
    SELECT 'Felbermeir' as last_name, 'Christian' as first_name, 'CARIAD' as brand, 'I/EG-132' as abteilung union all
    SELECT 'Lancier' as last_name, 'Stephan' as first_name, 'CARIAD' as brand, 'T2-AI' as abteilung union all
    SELECT 'Li' as last_name, 'Yan' as first_name, 'Mobility Asia' as brand, 'C|TB-31' as abteilung union all
    SELECT 'Talmelli' as last_name, 'Marta' as first_name, 'CARIAD' as brand, 'IDG steered by TH-82' as abteilung union all
    SELECT 'Oberst' as last_name, 'Andreas' as first_name, 'CARIAD' as brand, 'T2-BD' as abteilung union all
    SELECT 'Lindner' as last_name, 'Benedikt' as first_name, 'CARIAD' as brand, 'T2-BD' as abteilung union all
    SELECT 'Schmitt' as last_name, 'Matthias' as first_name, 'CARIAD' as brand, 'T2-BD' as abteilung union all
    SELECT 'Friedrich' as last_name, 'Andreas' as first_name, 'CARIAD' as brand, 'T2-BD' as abteilung union all
    SELECT 'Richter' as last_name, 'Thomas' as first_name, 'CARIAD' as brand, 'T2-BD' as abteilung union all
    SELECT 'Rheindt' as last_name, 'Ralf' as first_name, 'Porsche' as brand, 'EIU3' as abteilung union all
    SELECT 'Lesslich' as last_name, 'Dirk' as first_name, 'Porsche' as brand, 'EII4' as abteilung union all
    SELECT 'Xu' as last_name, 'Jiaqi (Extern)' as first_name, 'Mobility Asia' as brand, 'TB-13' as abteilung union all
    SELECT 'Wurm' as last_name, 'Andeas' as first_name, 'CARIAD' as brand, 'T1-34' as abteilung union all
    SELECT 'Baldus' as last_name, 'John' as first_name, 'CARIAD' as brand, 'T2-BE' as abteilung union all
    SELECT 'Marggraf' as last_name, 'Christian' as first_name, 'CARIAD' as brand, 'T1-73' as abteilung union all
    SELECT 'Sander' as last_name, 'Christian' as first_name, 'CARIAD' as brand, 'TH-84' as abteilung union all
    SELECT 'Hackl' as last_name, 'Uwe' as first_name, 'CARIAD' as brand, 'TH-84' as abteilung union all
    SELECT 'Radde' as last_name, 'Sven' as first_name, 'CARIAD' as brand, 'TH-86' as abteilung union all
    SELECT 'Yan' as last_name, 'Shenhao' as first_name, 'CARIAD China' as brand, 'C|TB-13' as abteilung
)
select 
    last_name,
    first_name,
    brand,
    abteilung
from cte_p;    

update people p
inner join temp_people tp
    on p.first_name = tp.first_name COLLATE utf8mb4_unicode_ci
    and p.last_name = tp.last_name COLLATE utf8mb4_unicode_ci
set
    p.department = tp.abteilung,
    p.brand = tp.brand,
    p.updated_at = now();
    
INSERT INTO people
(
    first_name, 
    last_name, 
    department, 
    created_at, 
    updated_at, 
    brand
)
select distinct
    tp.first_name,
    tp.last_name,
    tp.abteilung,
    now(),
    now(),
    brand
from temp_people tp
where not EXISTS
(
    select *
    from people p
    where
        p.first_name = tp.first_name COLLATE utf8mb4_unicode_ci
        and p.last_name = tp.last_name COLLATE utf8mb4_unicode_ci
);

DROP TEMPORARY TABLE IF EXISTS temp_roles;
CREATE TEMPORARY TABLE temp_roles
with cte_lead as
(
    SELECT 'SWC1147' as swc, 'Mikolajczyk, Marta' as name union all
    SELECT 'SWC980' as swc, 'Herberth, Roland' as name union all
    SELECT 'SWC1045' as swc, 'Weitensfelder, Christin' as name union all
    SELECT 'SWC1046' as swc, 'Weitensfelder, Christin' as name union all
    SELECT 'SWC1099' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1100' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1101' as swc, 'Sandner, Christian' as name union all
    SELECT 'SWC1102' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1103' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1104' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1105' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1106' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1107' as swc, 'Anders, Fabian' as name union all
    SELECT 'SWC1109' as swc, 'Depner, Daniel' as name union all
    SELECT 'SWC1110' as swc, 'Koeppl, Florian' as name union all
    SELECT 'SWC1112' as swc, 'Klein, Markus' as name union all
    SELECT 'SWC1113' as swc, 'Klein, Markus' as name union all
    SELECT 'SWC1114' as swc, 'Chittoju, Ravi' as name union all
    SELECT 'SWC1118' as swc, 'Kaiser, Shoma-Jakob' as name union all
    SELECT 'SWC1120' as swc, 'Jais, Andreas' as name union all
    SELECT 'SWC1125' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1126' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1127' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1136' as swc, 'Puetz, Simon' as name union all
    SELECT 'SWC1137' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1138' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1139' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1141' as swc, 'Jais, Andreas' as name union all
    SELECT 'SWC1142' as swc, 'Mikolajczyk, Marta' as name union all
    SELECT 'SWC1145' as swc, 'Anders, Fabian' as name union all
    SELECT 'SWC1150' as swc, 'Prankl, Wolfgang' as name union all
    SELECT 'SWC1151' as swc, 'Depner, Daniel' as name union all
    SELECT 'SWC1155' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1156' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1157' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1158' as swc, 'Salich, Sebastian' as name union all
    SELECT 'SWC1158' as swc, 'Salich, Sebastian' as name union all
    SELECT 'SWC1183' as swc, 'Mikolajczyk, Marta' as name union all
    SELECT 'SWC1184' as swc, 'Mikolajczyk, Marta' as name union all
    SELECT 'SWC1188' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1260' as swc, 'Shivananjaiah, Sudarshan' as name union all
    SELECT 'SWC1261' as swc, 'Shivananjaiah, Sudarshan' as name union all
    SELECT 'SWC1023' as swc, 'Koeppl, Florian' as name union all
    SELECT 'SWC1024' as swc, 'Otto, Oliver' as name union all
    SELECT 'SWC1027' as swc, 'Carls, Carl-Aron' as name union all
    SELECT 'SWC1031' as swc, 'Cao, Iijia' as name union all
    SELECT 'SWC1032' as swc, 'Cao, Iijia' as name union all
    SELECT 'SWC1037' as swc, 'Koeppl, Florian' as name union all
    SELECT 'SWC1054' as swc, 'Koch, Ronald' as name union all
    SELECT 'SWC1055' as swc, 'Carls, Carl-Aron' as name union all
    SELECT 'SWC1060' as swc, 'Li, Wenfei' as name union all
    SELECT 'SWC1061' as swc, 'Li, Wenfei' as name union all
    SELECT 'SWC1067' as swc, 'Carls, Carl-Aron' as name union all
    SELECT 'SWC1069' as swc, 'Koeppl, Florian' as name union all
    SELECT 'SWC1070' as swc, 'Koeppl, Florian' as name union all
    SELECT 'SWC1108' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1115' as swc, 'Roehlre, Alexander' as name union all
    SELECT 'SWC1134' as swc, 'Ejaz, Muhammad Moeed' as name union all
    SELECT 'SWC1140' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1143' as swc, 'Mikolajczyk, Marta' as name union all
    SELECT 'SWC1144' as swc, 'Mikolajczyk, Marta' as name union all
    SELECT 'SWC1146' as swc, 'Mikolajczyk, Marta' as name union all
    SELECT 'SWC1149' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1181' as swc, 'Roehlre, Alexander' as name union all
    SELECT 'SWC1182' as swc, 'Roehlre, Alexander' as name union all
    SELECT 'SWC1263' as swc, 'Hibler, Daniel' as name union all
    SELECT 'SWC1264' as swc, 'Hibler, Daniel' as name union all
    SELECT 'SWC994' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC995' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC996' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC997' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC998' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC1009' as swc, 'Auhuber, Daniel' as name union all
    SELECT 'SWC1016' as swc, 'Felbermeir, Christian' as name union all
    SELECT 'SWC1017' as swc, 'Lancier, Stephan' as name union all
    SELECT 'SWC1018' as swc, 'Lancier, Stephan' as name union all
    SELECT 'SWC1040' as swc, 'Li, Yan' as name union all
    SELECT 'SWC1041' as swc, 'Li, Yan' as name union all
    SELECT 'SWC1047' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC1048' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC1049' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC1050' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC1051' as swc, 'Boesl, Mathias' as name union all
    SELECT 'SWC1111' as swc, 'Talmelli, Marta' as name union all
    SELECT 'SWC1121' as swc, 'Jais, Andreas' as name union all
    SELECT 'SWC1122' as swc, 'Depner, Daniel' as name union all
    SELECT 'SWC1129' as swc, 'Heidenfelder, Jonas' as name union all
    SELECT 'SWC1153' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1185' as swc, 'Lancier, Stephan' as name union all
    SELECT 'SWC1186' as swc, 'Lancier, Stephan' as name union all
    SELECT 'SWC1187' as swc, 'Lancier, Stephan' as name union all
    SELECT 'SWC1265' as swc, 'Oberst, Andreas' as name union all
    SELECT 'SWC1266' as swc, 'Lindner, Benedikt' as name union all
    SELECT 'SWC1268' as swc, 'Schmitt, Matthias' as name union all
    SELECT 'SWC1269' as swc, 'Schmitt, Matthias' as name union all
    SELECT 'SWC1270' as swc, 'Friedrich, Andreas' as name union all
    SELECT 'SWC1271' as swc, 'Friedrich, Andreas' as name union all
    SELECT 'SWC1272' as swc, 'Friedrich, Andreas' as name union all
    SELECT 'SWC1274' as swc, 'Richter, Thomas' as name union all
    SELECT 'SWC1275' as swc, 'Oberst, Andreas' as name union all
    SELECT 'SWC1292' as swc, 'Rheindt, Ralf' as name union all
    SELECT 'SWC1004' as swc, 'Li, Wenfei' as name union all
    SELECT 'SWC1066' as swc, 'Lancier, Stephan' as name union all
    SELECT 'SWC1178' as swc, 'Lesslich, Dirk' as name union all
    SELECT 'SWC991' as swc, 'Xu, Jiaqi (Extern)' as name union all
    SELECT 'SWC992' as swc, 'Xu, Jiaqi (Extern)' as name union all
    SELECT 'SWC1005' as swc, 'Wurm, Andeas' as name union all
    SELECT 'SWC1020' as swc, 'Baldus, John' as name union all
    SELECT 'SWC1021' as swc, 'Baldus, John' as name union all
    SELECT 'SWC1029' as swc, 'Xu, Jiaqi (Extern)' as name union all
    SELECT 'SWC1030' as swc, 'Xu, Jiaqi (Extern)' as name union all
    SELECT 'SWC1044' as swc, 'Wurm, Andeas' as name union all
    SELECT 'SWC1045' as swc, 'Marggraf, Christian' as name union all
    SELECT 'SWC1046' as swc, 'Marggraf, Christian' as name union all
    SELECT 'SWC1056' as swc, 'Koeppl, Florian' as name union all
    SELECT 'SWC1057' as swc, 'Koeppl, Florian' as name union all
    SELECT 'SWC1083' as swc, 'Sander, Christian' as name union all
    SELECT 'SWC1084' as swc, 'Chittoju, Ravi' as name union all
    SELECT 'SWC1085' as swc, 'Chittoju, Ravi' as name union all
    SELECT 'SWC1088' as swc, 'Hackl, Uwe' as name union all
    SELECT 'SWC1089' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1090' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1091' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1092' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1093' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1094' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1095' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1096' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1097' as swc, 'Mukherjee, Annwesh' as name union all
    SELECT 'SWC1098' as swc, 'Radde, Sven' as name union all
    SELECT 'SWC1073' as swc, 'Yan, Shenhao' as name
)
select 
    swc,
    name,
    REPLACE(swc, 'SWC', '') as swc_id,
    p.id as person_id,
    ROW_NUMBER() over (partition by swc order by p.id desc) as rn
from cte_lead
left join people p 
    on CONCAT(p.last_name, ', ', p.first_name) = name;

update swcs_people_roles sp
inner join temp_roles tr
    on tr.swc_id = sp.swc_id
set
    sp.person_id = tr.person_id,
    sp.updated_at = now()
where 
    tr.rn = 1
    and tr.person_id is not null;

INSERT IGNORE INTO swcs_people_roles
(
    swc_id, 
    person_id, 
    created_at, 
    updated_at, 
    st12
)
select distinct
    swc_id,
    person_id,
    now(),
    now(),
    0
from temp_roles
where 
    person_id is not null;


-- =======================================
-- deactivate swcs
-- =======================================

with cte_swc as
(
    SELECT 'SWC1065' as swc union all
    SELECT 'SWC1118' as swc union all
    SELECT 'SWC1064' as swc union all
    SELECT 'SWC1044' as swc union all
    SELECT 'SWC1043' as swc union all
    SELECT 'SWC1042' as swc union all
    SELECT 'SWC1013' as swc union all
    SELECT 'SWC1012' as swc union all
    SELECT 'SWC1009' as swc union all
    SELECT 'SWC1117' as swc union all
    SELECT 'SWC1246' as swc union all
    SELECT 'SWC1308' as swc union all
    SELECT 'SWC1309' as swc union all
    SELECT 'SWC1310' as swc union all
    SELECT 'SWC1311' as swc
), cte_rep as
(
    select
        REPLACE(cte.swc, 'SWC', '')as swc_id
    from cte_swc cte
)
update swcs s
inner join cte_rep cte
    on cte.swc_id = s.id
set s.st12 = 0;

ROLLBACK;
