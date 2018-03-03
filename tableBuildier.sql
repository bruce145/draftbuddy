create table EventCurrent
(
	eId int not null identity
		primary key,
	gid int not null,
	tid int not null,
	pid int not null,
	eventType varchar(50) not null,
	secondaryType varchar(50),
	result varchar(50),
	x int not null,
	y int not null,
	gamePeriod int not null,
	time int not null,
	a1Pid int,
	a2Pid int,
	oppPid int,
	oppTid int not null,
	PIM int,
	emptyNet bit not null,
	gameWinner bit not null,
	shootout bit not null,
	strength varchar(50) not null
)
go

create index EventCurrent_pid_index
	on EventCurrent (pid)
go

create table GameLogCurrent
(
	gId int not null
		primary key,
	venue varchar(255),
	hTid int not null,
	hG int not null,
	hShot int not null,
	hGoaliePull bit not null,
	hW bit not null,
	hL bit not null,
	hOTW bit not null,
	hOTL bit not null,
	aTid int not null,
	aG int not null,
	aShot int not null,
	aGoaliePull bit not null,
	aW bit not null,
	aL bit not null,
	aOTW bit not null,
	aOTL bit not null,
	date datetime
)
go

create table PlayerCurrent
(
	pid int not null
		primary key,
	lastName varchar(255) not null,
	firstName varchar(255) not null,
	jerseyNo int not null,
	position varchar(5) not null,
	height varchar(10) not null,
	weight varchar(10) not null,
	birthDate varchar(255),
	age int not null,
	birthCountry varchar(255) not null,
	isRookie varchar(10) not null,
	tid int,
	shoots varchar(5),
	captain varchar(5),
	aCaptain varchar(5)
)
go

create table PlayerGameLogCurrent
(
	logid int not null identity
		primary key,
	gid int not null,
	tid int not null,
	pid int not null,
	G int not null,
	A int not null,
	Sh int not null,
	PPG int not null,
	PPA int not null,
	PlsMinus int not null,
	SHG int not null,
	SHA int not null,
	FOW int not null,
	FOTaken int not null,
	takeAway int not null,
	giveAway int not null,
	PIM int not null,
	Ht int not null,
	Blk int not null,
	TOI int,
	SHTOI int,
	PPTOI int,
	GA int,
	Saves int,
	evSaves int,
	evGA int,
	shSaves int,
	shGA int,
	ppSaves int,
	ppGA int,
	Win int,
	Loss int
)
go

create index PlayerGameLogCurrent_pid_index
	on PlayerGameLogCurrent (pid)
go

create index PlayerGameLogCurrent_tid_index
	on PlayerGameLogCurrent (tid)
go

create table TeamCurrent
(
	id int not null
		primary key,
	abbreviation varchar(255) not null,
	city varchar(255) not null,
	name varchar(255) not null,
	venue varchar(255) not null,
	conference varchar(255) not null,
	division varchar(255) not null
)
go

create unique index TeamCurrent_id_uindex
	on TeamCurrent (id)
go

create table TeamGameLogCurrent
(
	logid int not null identity
		primary key,
	gid int not null,
	tid int not null,
	W int not null,
	L int not null,
	G int not null,
	Sh int not null,
	PPG int not null,
	SHG int not null,
	FOW int not null,
	FOTaken int not null,
	takeAway int not null,
	giveAway int not null,
	PIM int not null,
	Ht int not null,
	Blk int not null,
	GA int,
	evGA int,
	shGA int,
	ppGA int
)
go

create index TeamGameLogCurrent_tid_index
	on TeamGameLogCurrent (tid)
go

CREATE VIEW FantasyStat2017 AS
  SELECT PlayerCurrent.pid,
    MAX(PlayerCurrent.lastName) AS "LastName",
    MAX(PlayerCurrent.firstName) AS "FirstName",
    COUNT(PlayerGameLogCurrent.logid) AS "GP",
    SUM(PlayerGameLogCurrent.G) AS "Goals",
    SUM(PlayerGameLogCurrent.A) AS "Assists",
    SUM(PlayerGameLogCurrent.PlsMinus) AS "+-",
    SUM(PlayerGameLogCurrent.PIM) AS "PIM",
    SUM(PlayerGameLogCurrent.PPA) + SUM(PlayerGameLogCurrent.PPG) AS "PPP",
    MAX(sub.GWG) AS "GWG",
    SUM(PlayerGameLogCurrent.Sh) AS "Shots",
    AVG(PlayerGameLogCurrent.TOI) AS "AvgTOI",
    SUM(PlayerGameLogCurrent.Win) AS "WINS",
    SUM(PlayerGameLogCurrent.GA) AS "GA",
    SUM(PlayerGameLogCurrent.Saves) AS "Saves"
  FROM (((SELECT pid, sum(CASE WHEN EventCurrent.gameWinner = 1 THEN 1 else 0 END) AS "GWG"
          FROM EventCurrent
          WHERE gid > 2017020000
          GROUP BY EventCurrent.pid) sub
    INNER JOIN PlayerCurrent on sub.pid = PlayerCurrent.pid)
    INNER JOIN PlayerGameLogCurrent ON sub.pid = PlayerGameLogCurrent.pid)
  WHERE gid > 2017020000
  GROUP BY PlayerCurrent.pid
go


