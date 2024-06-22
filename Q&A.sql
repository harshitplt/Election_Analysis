use election;

-- Q1. 
(
with 
sub_query_1 as 
(
select 
pc_name, 
(sum(total_votes) / sum(distinct(total_electors)) * 100) as per_14
from resultant_2014 
group by pc_name
),
sub_query_2 as 
(
select 
pc_name, 
(sum(total_votes) / sum(distinct(total_electors)) * 100) as per_19
from resultant_2019 
group by pc_name
)
(
(
select 
c19.pc_name as constituencies,
concat(round(c19.per_19, 2), '%') as per_19,
concat(round(c14.per_14, 2), '%') as per_14
from sub_query_2 c19 
join sub_query_1 c14 on c19.pc_name = c14.pc_name
order by c19.per_19 desc, c14.per_14 desc 
limit 5
) 
union all
(
select 
c19.pc_name as constituencies,
concat(round(c19.per_19, 2), '%') as per_19,
concat(round(c14.per_14, 2), '%') as per_14
from sub_query_2 c19 
join sub_query_1 c14 on c19.pc_name = c14.pc_name
order by c19.per_19 asc, c14.per_14 asc 
limit 5
)));

-- Q2
(
with sub_query_1 as 
(
select 
state, 
(sum(total_votes) / sum(distinct(total_electors)) * 100) as per_14
from resultant_2014 
group by state
),
sub_query_2 as 
(
select 
state, 
(sum(total_votes) / sum(distinct(total_electors)) * 100)  as per_19
from resultant_2019 
group by state
)
(
(
select 
c19.state,
concat(round(c19.per_19, 2), '%') as per_19,
concat(round(c14.per_14, 2), '%') as per_14
from sub_query_2 c19 
join sub_query_1 c14 on c19.state = c14.state
order by c19.per_19 desc, c14.per_14 desc limit 5
) 
union all
(
select 
c19.state,
concat(round(c19.per_19, 2), '%') as per_19,
concat(round(c14.per_14, 2), '%') as per_14
from sub_query_2 c19 
join sub_query_1 c14 on c19.state = c14.state
order by c19.per_19 asc, c14.per_14 asc limit 5
)));

-- Q3
(
with 
sub_query_1 as 
(
select 
pc_name,
state,
party,
(sum(total_votes) / sum(total_electors) * 100) as per_14
from resultant_2014 
group by pc_name, party, state
), 
sub_query_2 as 
(
select 
pc_name,
state,
party,
(sum(total_votes) / sum(total_electors) * 100) as per_19
from resultant_2019 
group by pc_name, party, state
),
sub_query_3_ranking as
(
select 
pc_name as constituencies,
state,
party,
per_14,
rank () over (partition by state order by per_14 desc) as ranks_2014
from sub_query_1
group by pc_name, state, party, per_14
order by state, ranks_2014 asc
),
sub_query_4_ranking as
(
select 
pc_name as constituencies,
state,
party,
per_19,
rank () over (partition by pc_name order by per_19 desc) as ranks_2019
from sub_query_2
order by pc_name, ranks_2019 asc
)
(
select a.constituencies, a.party, a.state, concat(round(b.per_19, 2), '%') as percent_19, concat(round(a.per_14, 2), '%') as percent_14
from sub_query_4_ranking b
left join sub_query_3_ranking a on a.constituencies = b.constituencies and a.state = b.state and a.party = b.party
where a.ranks_2014 = 1 and b.ranks_2019 = 1
order by b.per_19 desc, a.per_14 desc
));

-- Q4
(
with 
sub_query_1 as
(
select 
pc_name,
party, 
(sum(total_votes) / sum(total_electors) * 100) as per_14
from resultant_2014 
group by pc_name, party
 ), 
sub_query_2 as 
(
select 
pc_name,
party, 
(sum(total_votes) / sum(total_electors) * 100) as per_19
from resultant_2019 
group by pc_name, party
),  
sub_query_3_ranking as
(
select 
pc_name as constituencies,
party as party_14,
per_14,
rank () over (order by per_14 desc) as ranks_2014
from sub_query_1
order by ranks_2014 desc, per_14 desc 
),
sub_query_4_ranking as
(
select 
pc_name as constituencies,
per_19,
party as party_19,
rank () over (order by per_19 desc) as ranks_2019
from sub_query_2
order by ranks_2019 desc, per_19 desc
),
winner as
(
select 
r19.constituencies,
r19.party_19 as party_19,
r19.per_19 as per_19,
r19.ranks_2019,
r14.party_14 as party_14,
r14.per_14 as per_14,
r14.ranks_2014,
(r19.per_19 - r14.per_14) as as_diff
from sub_query_4_ranking r19
join sub_query_3_ranking r14 on r19.constituencies = r14.constituencies and r19.party_19 = r14.party_14
)
(
select 
constituencies,
party_19,
concat(round(per_19, 2), '%') as percent_2019,
concat(round(per_14, 2), '%') as percent_2014,
concat(round(as_diff, 2), '%') as difference
from winner
order by as_diff desc 
limit 10
));

-- Q5
(
with runner_up_2019 as
(
with 
sub_query_1 as
(
select candidate, party,
(sum(total_votes) / sum(total_electors) * 100) as per_19,
rank() over(partition by party order by (sum(total_votes) / sum(total_electors) * 100) desc) as rank_19
from resultant_2019
group by candidate, party
order by party, rank_19 asc
)
(
select 
r2.candidate, 
r2.party, (coalesce(r1.per_19 , 0) - coalesce(r2.per_19, 0)) as margin_2019
from 
(
select candidate, party, per_19, rank_19
from sub_query_1
where rank_19 = 2
group by candidate, party, per_19, rank_19
) r2
join
(
select candidate, party, per_19, rank_19
from sub_query_1
where rank_19 = 1
group by candidate, party, per_19, rank_19
) r1 on r2.party = r1.party
order by margin_2019 desc
)), 
runner_up_2014 as
(
with 
sub_query_1 as
(
select candidate, party,
(sum(total_votes) / sum(total_electors) * 100) as per_14,
rank() over(partition by party order by (sum(total_votes) / sum(total_electors) * 100) desc) as rank_14 
from resultant_2014
group by candidate, party
order by party, rank_14 asc
)
(
select r2.candidate, r2.party, 
(coalesce(r1.per_14 , 0) - coalesce(r2.per_14, 0)) as margin_2014
from 
(
select candidate, party, per_14, rank_14
from sub_query_1
where rank_14 = 2
group by candidate, party, per_14, rank_14
) r2
join
(
select candidate, party, per_14, rank_14
from sub_query_1
where rank_14 = 1
group by candidate, party, per_14, rank_14
) r1 on r2.party = r1.party
order by margin_2014 desc
))
(
select 
wu19.candidate, 
wu19.party, 
concat(round(wu19.margin_2019, 2), '%') as margin_2019, 
concat(round(wu14.margin_2014, 2), '%') as margin_2014, 
concat(round((wu19.margin_2019 - wu14.margin_2014), 2), '%') as margin_diff
from runner_up_2019 wu19
join runner_up_2014 wu14 on wu19.party = wu14.party
order by margin_diff desc
limit 5
)
);

-- Q6
(
with sub_query_1 as
(
select 
party,
(sum(total_votes)  / sum(total_electors) * 100) as per_14
from resultant_2014
group by party
order by per_14 desc
),
sub_query_2 as 
(
select
party,
(sum(total_votes) / sum(total_electors) *100) as per_19
from resultant_2019
group by party
order by per_19 desc
),
com_2014 as 
(
select
sq2.party,
concat(round(sq1.per_14, 2), '%') as per_14,
concat(round(coalesce(sq2.per_19, 0), 2), '%') as per_19
from sub_query_1 sq1
left join sub_query_2 sq2 on sq1.party = sq2.party
order by per_19 desc, per_14 desc
),
com_2019 as 
(
select 
sq2.party,
concat(round(sq2.per_19, 2), '%') as per_19,
concat(round(coalesce(sq1.per_14, 0), 2), '%') as per_14
from sub_query_2 sq2
left join sub_query_1 sq1 on sq2.party = sq1.party
order by per_19 desc, per_14 desc
)
(
select
*
from com_2014
union all 
select 
*
from com_2019
));

-- Q7
(
with sub_query_1 as
(
select 
party,
state,
(sum(total_votes) / sum(total_electors) * 100) as per_14
from resultant_2014
group by party, state
order by per_14 desc
),
sub_query_2 as 
(
select
party,
state,
(sum(total_votes) / sum(total_electors) * 100) as per_19
from resultant_2019
group by party, state
order by per_19 desc
),
com_2014 as 
(
select
sq1.state,
sq1.party,
sq1.per_14,
coalesce(sq2.per_19, 0) as per_19
from sub_query_1 sq1
left join sub_query_2 sq2 on sq1.state = sq2.state and sq1.party = sq2.party
),
com_2019 as 
(
select 
sq2.state,
sq2.party,
sq2.per_19,
coalesce(sq1.per_14, 0) as per_14
from sub_query_2 sq2
left join sub_query_1 sq1 on sq2.state = sq1.state and sq1.party = sq2.party
)
(
select
c19.party,
concat(round(c19.per_19, 2), '%') as per_2019,
concat(round(c14.per_14, 2), '%') as per_2014,
c14.state
from com_2019 c19
left join com_2014 c14
on c14.party = c19.party
order by c19.per_19 desc, c14.per_14 desc
));


-- Q8
(
with 
party_rank_14 as
(
select pc_name, party, 
(sum(total_votes) / sum(total_electors) * 100) as per_14,
rank () over (partition by party order by (sum(total_votes) / sum(total_electors) * 100) desc) as rank_14
from resultant_2014 
group by pc_name, party
order by per_14 desc
),
party_rank_19 as
(
select pc_name, party, 
(sum(total_votes) / sum(total_electors) * 100) as per_19,
rank () over (partition by party order by (sum(total_votes) / sum(total_electors) * 100) desc) as rank_19
from resultant_2019 
group by pc_name, party
),
nat_pty as
(
select pr19.pc_name as constituencies, pr19.party as party, (pr19.per_19 - pr14.per_14) as diff,
pr19.rank_19, pr14.rank_14,
rank () over (partition by pr19.pc_name order by (pr19.per_19 - pr14.per_14) desc) as diff_rank
from party_rank_19 pr19
join party_rank_14 pr14 on pr19.party = pr14.party and pr19.rank_19 = pr14.rank_14
order by diff_rank asc
)
(
select constituencies, party, concat(round(diff, 2), '%') as diff
from nat_pty 
where diff_rank < 6 and rank_19 < 3
order by diff desc
)
);

-- Q9
(
with 
party_rank_14 as
(
select pc_name, party, (sum(total_votes) / sum(total_electors) * 100) as per_14,
rank () over (partition by party order by (sum(total_votes) / sum(total_electors) * 100) desc) as rank_14
from resultant_2014 
group by pc_name, party
order by per_14 desc
),
party_rank_19 as
(
select pc_name, party, (sum(total_votes) / sum(total_electors) * 100) as per_19,
rank () over (partition by party order by (sum(total_votes) / sum(total_electors) * 100) desc) as rank_19
from resultant_2019 
group by pc_name, party
order by per_19 desc
),
nat_pty as
(
select pr19.pc_name as constituencies, pr19.party as party, (pr19.per_19 - pr14.per_14) as diff,
pr19.rank_19, pr14.rank_14,
rank () over (partition by pr19.pc_name order by (pr19.per_19 - pr14.per_14) asc) as diff_rank
from party_rank_19 pr19
join party_rank_14 pr14 on pr19.party = pr14.party and pr19.rank_19 = pr14.rank_14
order by pr19.pc_name, diff_rank asc
)
(
select constituencies, party, concat(round(diff, 2), '%') as diff, diff_rank, rank_19
from nat_pty 
where diff_rank < 6 and rank_19 < 3
order by diff_rank asc, diff desc
)
);

-- Q10
(
select 
r14.pc_name as constituencies, r14.party, concat(round((r14.total_votes / r14.total_electors * 100), 2), '%') as per_2014, 
(
select concat(round(total_votes / total_electors * 100, 2), '%') as per_2019 
from resultant_2019 
where party = 'NOTA' 
order by per_2019 desc 
limit 1) as per_2019
from resultant_2014 r14
join resultant_2019 r19 on r19.pc_name = r14.pc_name 
where r19.party = 'NOTA' and r14.party = 'NOTA' 
order by per_2014 desc
limit 1
);

-- Q11
(
with check_1 as
(
select party, 
(sum(total_votes) / sum(total_electors) * 100) as per
from resultant_2019
group by party
order by per desc
),
check_2 as
(
select party, state, 
(sum(total_votes) / sum(total_electors) * 100) as per
from resultant_2019
group by party, state
order by per desc
),
check_const as
(
select pc_name, 
(sum(total_votes) / sum(total_electors) * 100) as per
from resultant_2019
group by pc_name
order by per desc
),
check_candi as
(
select candidate, pc_name, 
(sum(total_votes) / sum(total_electors) * 100) as per
from resultant_2019
group by candidate, pc_name
order by per desc
),
candi_const as
(
select distinct 
cc2.candidate, cc2.pc_name, cc2.per 
from check_candi cc2 
left join check_const cc1 on cc1.pc_name = cc2.pc_name and cc1.per = cc2.per
),
pty_st as
(
select distinct 
c2.party, c2.state, c1.per 
from check_2 c2 
left join check_1 c1 on c1.party = c2.party and c1.per = c2.per
)
(
select distinct 
cc.candidate, cc.pc_name as constituencies, concat(round(ps.per, 2), '%') as per, ps.party, ps.state
from pty_st ps 
left join candi_const cc on cc.per = ps.per
where cc.per <= 10.000 and ps.per <= 10.000
order by per desc));