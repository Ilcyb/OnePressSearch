create database if not exists search_engine;

use search_engine;

create table if not exists url_hash(
    id int not null auto_increment,
    url_hash varchar(32) not null,
    content text not null,
    primary key(id)
)engine=InnoDB default charset=utf8;

alter table url_hash add key(url_hash(20));