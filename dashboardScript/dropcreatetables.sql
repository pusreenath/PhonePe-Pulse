-- Created by: @pusreenath https://github.com/pusreenath on 6th May 2023

create database phonepe_pulse

drop table if exists agg_tdata
create table agg_tdata (
name varchar(50) not null,
count int not null,
amount float not null,
state varchar(50) not null,
year int not null,
quarter int not null)

drop table if exists agg_udata
create table agg_udata (
brand varchar(50) not null,
count int not null,
data_aggregated_registeredUsers int not null,
data_aggregated_appOpens bigint not null,
state varchar(50) not null,
year int not null,
quarter int not null)

drop table if exists districts
create table districts (
entityName varchar(50) not null,
metric_count int not null,
metric_amount float not null,
state varchar(50) not null,
year int not null,
quarter int not null,
name varchar(50) not null,
registeredUsers int not null,
transaction_or_users varchar(50) not null)

drop table if exists pincodes
create table pincodes (
entityName varchar(50) null,
metric_count int not null,
metric_amount float not null,
state varchar(50) not null,
year int not null,
quarter int not null,
transaction_or_users varchar(50) not null,
name varchar(50) null,
registeredUsers int not null)

drop table if exists merged_geolocation_data
create table merged_geolocation_data (
state varchar(50)  not null,
state_latitude float null,
state_longitude float null,
district varchar(50) null,
district_latitude float null,
district_longitude float null,
year int not null,
quarter int  not null,
transactioncategory varchar(50) null,
totaltransactioncount numeric  null,
totaltransactionamount float null,
totaltransactioncount_district numeric  null,
totaltransactionamount_district float null,
transaction_or_users varchar(50) null,
brand varchar(50) null,
totaldevicecount numeric  null,
totalaggregisteredusers numeric  null,
totalaggappopens numeric  null,
totalregisteredusers_district numeric  null
)