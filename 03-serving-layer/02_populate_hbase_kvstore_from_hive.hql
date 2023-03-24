create external table vigneshv_AllUsersTopMovieReco_Hbase (
    userid_movieid string,
    title string,
    genres string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,reco:title,reco:genres')
TBLPROPERTIES ('hbase.table.name' = 'vigneshv_all_users_top_movie_reco');

insert overwrite table vigneshv_AllUsersTopMovieReco_Hbase
select 
    concat(userid,"_",movieid),
    title, 
    genres
from vigneshv_AllUsersTopMovieRecos;