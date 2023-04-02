create table product
(
    code         varchar(30)  not null
        constraint product_pkey
            primary key,
    product_name varchar(100) not null
);

create table supply_center
(
    center_name   varchar(30) not null
        constraint supply_center_center_name_key
            unique,
    director_name varchar(30) not null
        constraint supply_center_director_name_key
            unique,
    constraint supply_center_key
        primary key (center_name, director_name)
);

create table model
(
    model_name   varchar(100) not null
        constraint model_pkey
            primary key,
    unit_price   integer      not null,
    product_code varchar(30)  not null
        constraint pn_fk
            references product
);

create table salesman
(
    number       integer     not null
        constraint salesman_pkey
            primary key,
    name         varchar(30),
    gender       varchar(30) not null,
    age          integer     not null,
    phone_number integer     not null
        constraint phone
            unique
);

create table location
(
    location_id serial
        constraint location_pkey
            primary key,
    city        varchar(30),
    country     varchar(30) not null,
    constraint ct
        unique (city, country)
);

create table client_enterprise
(
    name          varchar(60) not null
        constraint client_enterprise_pkey
            primary key,
    industry      varchar(70) not null,
    location_id   integer
        constraint loc
            references location,
    supply_center varchar(30) not null
        constraint supply_fk
            references supply_center (center_name)
);

create table contract
(
    number            varchar(30) not null
        constraint contract_pkey
            primary key,
    contract_date     date        not null,
    client_enterprise varchar(60) not null
        constraint client_fk
            references client_enterprise
);

create table contract_order
(
    id                      integer      not null,
    estimated_delivery_date date         not null,
    lodgement_date          date,
    quantity                integer      not null,
    model                   varchar(100) not null
        constraint pro_fk
            references model,
    salesman_number         integer      not null
        constraint sn_fk
            references salesman,
    contract_number         varchar(30)  not null
        constraint order_fk
            references contract,
    constraint order_pk
        primary key (id, contract_number)
);


