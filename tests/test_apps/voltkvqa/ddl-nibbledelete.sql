LOAD CLASSES voltkv.jar;

CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, inserttime     timestamp DEFAULT NOW NOT NULL
, f1        varchar(62) default 'a'
, f2        varchar(62) default 'b'
, f3        varchar(62) default 'c'
, f4        varchar(62) default 'a'
, f5        varchar(62) default 'b'
, f6        varchar(62) default 'c'
, f7        varchar(62) default 'a'
, f8        varchar(62) default 'b'
, f9        varchar(62) default 'c'
, f10        varchar(62) default 'a'
, f11        varchar(62) default 'b'
, f12        varchar(62) default 'c'
, f13        varchar(62) default 'a'
, f14        varchar(62) default 'b'
, f15        varchar(62) default 'c'
, f16        varchar(62) default 'a'
, f17        varchar(62) default 'b'
, f18        varchar(62) default 'c'
, f19        varchar(62) default 'a'
, f20        varchar(62) default 'b'
, f21        varchar(62) default 'c'
, f22        varchar(62) default 'a'
, f23        varchar(62) default 'b'
, f24        varchar(62) default 'c'
, f25        varchar(62) default 'a'
, f26        varchar(62) default 'b'
, f27        varchar(62) default 'c'
, f28        varchar(62) default 'a'
, f29        varchar(62) default 'b'
, f30        varchar(62) default 'c'
, PRIMARY KEY (key)
,
);
PARTITION TABLE store ON COLUMN key;

CREATE INDEX inserttimeidx ON store ( inserttime );
CREATE PROCEDURE FROM class voltkvqa.procedures.Initialize;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Get;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Put;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Remove;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.GetMp;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.PutMp;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.PutTS;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.PutMpTS;
