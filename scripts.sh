hbase shell

create ' transaction_lookup_table ', 'TransactionDetails', 'CardDetails', 'MemberDetails'
scan 'transaction_lookup_table'

exit

sqoop import --connect jdbc:mysql://upgradawsrds1.cyaielc9bmnf.us-east-1.rds.amazonaws.com:3306/cred_financials_data --username upgraduser --password upgraduser --table card_member --hbase-table transaction_lookup_table --column-family CardDetails --hbase-row-key card_id -m 1

sqoop import --connect jdbc:mysql://upgradawsrds1.cyaielc9bmnf.us-east-1.rds.amazonaws.com:3306/cred_financials_data --username upgraduser --password upgraduser --query "select c.card_id, m.member_id ,m.score from cred_financials_data.card_member c join cred_financials_data.member_score m on c.member_id = m.member_id where \$CONDITIONS" --hbase-table transaction_lookup_table --column-family MemberDetails --hbase-row-key card_id -m 1

hive

drop table card_transactions_ext;
drop table card_transactions_rank;
drop table card_stage;
drop table card_lookup;

create external table card_transactions_ext(`card_id` string, `member_id` int, `amount` int ,`postcode` int, `pos_id` int, `transaction_dt` string, `status` string) row format delimited fields terminated by ',' ;
load data local inpath '/home/ec2-user/capstone/data/card_transactions.csv' overwrite into table card_transactions_ext;
select * from card_transactions_ext limit 10;

create  table card_transactions_rank AS select card_id, amount, transaction_dt, postcode,rank from ( select card_id, amount, transaction_dt, postcode ,rank() over ( partition by card_id order by UNIX_TIMESTAMP(transaction_dt) desc) as rank  from card_transactions_ext where status='GENUINE' ) t where rank < 10 ;
select * from card_transactions_rank limit 10;

create  table card_stage AS select card_id, AVG(amount) as mov_avg , STDDEV(amount) as std from card_transactions_rank group by card_id;
select * from card_stage limit 10;

create  table card_lookup AS select s.card_id, cast( (3 * s.std)+ s.mov_avg as decimal (12,2)) as ucl , r.postcode , r.transaction_dt   from card_stage s , card_transactions_rank r where r.rank =1 and r.card_id =s.card_id;



create external table transaction_lookup_hive(card_id string, postcode int, transaction_dt string, ucl double) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with serdeproperties ("hbase.columns.mapping"=":key,TransactionDetails:postcode,TransactionDetails:transaction_dt,TransactionDetails:ucl") tblproperties("hbase.table.name"="transaction_lookup_table");

insert overwrite table transaction_lookup_hive select card_id, postcode , transaction_dt , ucl from card_lookup;


exit


