--drop table if exists aud.despesasOrcadasEmpenhadas;

create table if not exists aud.auddespesasOrcadasEmpenhadas(
   id bigint,
   cdentidade bigint,
   idtce bigint,
   nrano bigint,
   idquadrimestre bigint,
   dsquadrimestre character varying(255),
   idclassificacaodespesa character varying(2),
   dsclassificacaodespesa character varying(2),
   valorOrcado numeric(16,4),
   valorRealizado numeric(16,4),
   status boolean,
   constraint pk_despesasOrcadasEmpenhadas primary key (id, cdentidade, nrano, idquadrimestre)   
);
 
--drop table if exists aud.audexercicioMes;

create table if not exists aud.audexercicioMes(
   id bigint,
   cdentidade bigint,
   idtce bigint,
   nrano bigint,
   nrmes character(2),
   situacao character(1),
   dataImportacao timestamp default now(),
   status boolean,
   constraint pk_audexercicioMes primary key (id, cdentidade, idtce, nrano, nrmes)   
);

